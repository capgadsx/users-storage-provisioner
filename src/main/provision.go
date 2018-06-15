package main

import (
	"os"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/kubernetes"
	"lib/controller"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/api/core/v1"
	"errors"
	"fmt"
	"strings"
	"path/filepath"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"flag"
	"github.com/golang/glog"
	"github.com/go-ldap/ldap"
	"strconv"
	"compress/gzip"
	"io"
	"archive/tar"
)

func main() {
	var provisionerName string
	var dataDirectory string
	var baseArchive string
	var nfsServer string
	var nfsPath string
	var ownerAnnotation string
	var ldapServer string
	var ldapBaseDN string
	var ldapUserFilter string
	var ldapUID string
	var ldapGID string
	flag.StringVar(&provisionerName, "name", "storage.example.com/custom", "The name of this provisioner")
	flag.StringVar(&dataDirectory, "data", "/data", "Path were pv's are created inside the container")
	flag.StringVar(&baseArchive, "base", "/data/base.tar.gz", "Archive containing the base directory tree to extract in the provisioned folder (only .tar.gz files supported for now)")
	flag.StringVar(&nfsServer, "server", "127.0.0.1", "NFS Server were pv's are stored ")
	flag.StringVar(&nfsPath, "path", "/exports/pvs", "NFS Path were pv's are stored")
	flag.StringVar(&ownerAnnotation, "ann", "storage.example.com/owner", "Annotation used to identify owner user of the provisioned pv")
	flag.StringVar(&ldapServer, "lServer", "ldap.example.com:389", "Address of LDAP Server where user data is stored")
	flag.StringVar(&ldapBaseDN, "lBase", "ou=users,o=example,c=com", "Base DN for user queries")
	flag.StringVar(&ldapUserFilter, "lFilter", "uid", "Query parameter to filter user, internally used in the form of (&({param}={username}))")
	flag.StringVar(&ldapUID, "lUID", "uidNumber", "LDAP attribute that contains the user uid")
	flag.StringVar(&ldapGID, "lGID", "uidNumber", "LDAP attribute that contains the user gid")
	flag.Parse()
	flag.Set("logtostderr", "true")
	glog.Info("Starting custom dynamic pv provisioner")
	glog.Infof("Flags:")
	glog.Infof("		-name: %v", provisionerName)
	glog.Infof("		-data: %v", dataDirectory)
	glog.Infof("		-base: %v", baseArchive)
	glog.Infof("		-server: %v", nfsServer)
	glog.Infof("		-path: %v", nfsPath)
	glog.Infof("		-ann: %v", ownerAnnotation)
	glog.Infof("		-lServer: %v", ldapServer)
	glog.Infof("		-lBase: %v", ldapBaseDN)
	glog.Infof("		-lFilter: %v", ldapUserFilter)
	glog.Infof("		-lUID: %v", ldapUID)
	glog.Infof("		-lGID: %v", ldapGID)
	config, err := rest.InClusterConfig()
	if err != nil {
		glog.Fatalf("Failed to get cluster config: %v", err)
	}
	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		glog.Fatalf("Failed to create kubernetes ClientSet: %v", err)
	}
	serverVersion, err := clientSet.Discovery().ServerVersion()
	if err != nil {
		glog.Fatalf("Error getting server version: %v", err)
	}
	provisioner := &CustomNFSUsersProvisioner{
		dataDirectory:   dataDirectory,
		server:          nfsServer,
		path:            nfsPath,
		ownerAnnotation: ownerAnnotation,
		baseArchive:     baseArchive,
		ldap: LDAPConfig{
			server:       ldapServer,
			baseDN:       ldapBaseDN,
			userFilter:   ldapUserFilter,
			uidAttribute: ldapUID,
			gidAttribute: ldapGID,
		},
	}
	provisionController := controller.NewProvisionController(clientSet, provisionerName, provisioner, serverVersion.GitVersion)
	provisionController.Run(wait.NeverStop)
}

type LDAPConfig struct {
	server       string
	baseDN       string
	userFilter   string
	uidAttribute string
	gidAttribute string
}

type CustomNFSUsersProvisioner struct {
	dataDirectory   string
	server          string
	path            string
	ownerAnnotation string
	baseArchive     string
	ldap            LDAPConfig
}

func (provisioner *CustomNFSUsersProvisioner) Provision(options controller.VolumeOptions) (*v1.PersistentVolume, error) {
	owner, found := options.PVC.ObjectMeta.Annotations[provisioner.ownerAnnotation]
	if !found {
		return nil, errors.New(fmt.Sprintf("missing '%v' annotation", provisioner.ownerAnnotation))
	}
	userUID, userGID, err := GetUserGidUid(owner, provisioner.ldap.server, provisioner.ldap.baseDN, provisioner.ldap.userFilter, provisioner.ldap.uidAttribute, provisioner.ldap.gidAttribute)
	if err != nil {
		return nil, err
	}
	glog.Infof("Creating new pv %v for user %v (uid: %v gid: %v)", options.PVName, owner, userUID, userGID)
	customPVName := strings.Join([]string{"pv", owner}, "-")
	pvRootPath := filepath.Join(provisioner.dataDirectory, customPVName)
	pvUserVolumePath := filepath.Join(pvRootPath, "volume")
	pvSuccessFlagPath := filepath.Join(pvRootPath, ".success")
	if _, err := os.Stat(pvSuccessFlagPath); os.IsNotExist(err) {
		if err := os.RemoveAll(pvRootPath); err != nil {
			return nil, errors.New(fmt.Sprintf("failed to remove directory %v (caused by %v)", pvRootPath, err))
		}
		glog.Infof("Creating path %v", pvUserVolumePath)
		if err := os.MkdirAll(pvUserVolumePath, 0740); err != nil {
			return nil, errors.New(fmt.Sprintf("failed to create directory %v (caused by %v)", pvUserVolumePath, err))
		}
		os.Chown(pvUserVolumePath, userUID, userGID)
		//TODO: Show progress while extracting
		if err = ExtractBase(provisioner.baseArchive, provisioner.dataDirectory, pvUserVolumePath, owner, userUID, userGID); err != nil {
			return nil, err
		}
		os.Create(pvSuccessFlagPath)
	}
	mountPath := filepath.Join(provisioner.path, customPVName, "volume")
	glog.Infof("NFS path for new PersistentVolumeSource: '%v:%v'", provisioner.server, mountPath)
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: options.PVName,
		},
		Spec: v1.PersistentVolumeSpec{
			AccessModes:                   options.PVC.Spec.AccessModes,
			PersistentVolumeReclaimPolicy: v1.PersistentVolumeReclaimDelete,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): options.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)],
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				NFS: &v1.NFSVolumeSource{
					Server: provisioner.server,
					Path:   mountPath,
				},
			},
		},
	}
	return pv, nil
}

func (provisioner *CustomNFSUsersProvisioner) Delete(volume *v1.PersistentVolume) error {
	glog.Infof("Deleting pv %v from database", volume.Name)
	//Since the volume contains the user home, we don't delete the files..
	return nil
}

func GetUserGidUid(username, ldapServerAddr, baseDN, userFilter, uidAttribute, gidAttribute string) (int, int, error) {
	ldapConnection, err := ldap.Dial("tcp", ldapServerAddr)
	if err != nil {
		return -1, -1, err
	}
	defer ldapConnection.Close()
	request := ldap.NewSearchRequest(baseDN,
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
		fmt.Sprintf("(&(%s=%s))", userFilter, username), []string{uidAttribute, gidAttribute}, nil)
	result, err := ldapConnection.Search(request)
	if err != nil {
		return -1, -1, err
	}
	uidString := result.Entries[0].GetAttributeValue(uidAttribute)
	gidString := result.Entries[0].GetAttributeValue(gidAttribute)
	uid, err := strconv.Atoi(uidString)
	if err != nil {
		return -1, -1, err
	}
	gid, err := strconv.Atoi(gidString)
	if err != nil {
		return -1, -1, err
	}
	return uid, gid, nil
}

func ExtractBase(archive, tmpFolder, target, owner string, uid, gid int) error {
	if !strings.HasSuffix(archive, "tar.gz") {
		return errors.New("unsupported archive format (only .tar.gz is supported at the moment)")
	}
	tmpTarPath := filepath.Join(tmpFolder, fmt.Sprintf("tmp-%s.tar", owner))
	if err := ExtractGZIP(archive, tmpTarPath); err != nil {
		return err
	}
	defer os.Remove(tmpTarPath)
	if err := ExtractTAR(tmpTarPath, target, uid, gid); err != nil {
		return err
	}
	return nil
}

func ExtractTAR(source, targetFolder string, uid, gid int) error {
	tarFile, err := os.Open(source)
	if err != nil {
		return err
	}
	defer tarFile.Close()
	reader := tar.NewReader(tarFile)
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		path := filepath.Join(targetFolder, header.Name)
		info := header.FileInfo()
		if info.IsDir() {
			if err = os.MkdirAll(path, info.Mode()); err != nil {
				return err
			}
			if err = os.Chown(path, uid, gid); err != nil {
				return err
			}
			continue
		}
		file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode())
		if err != nil {
			return err
		}
		_, err = io.Copy(file, reader)
		file.Close()
		if err != nil {
			return err
		}
		if err = os.Chown(path, uid, gid); err != nil {
			return err
		}
	}
	return nil
}

func ExtractGZIP(source, target string) error {
	file, err := os.Open(source)
	if err != nil {
		return err
	}
	defer file.Close()
	reader, err := gzip.NewReader(file)
	if err != nil {
		return err
	}
	defer reader.Close()
	targetFile, err := os.Create(target)
	if err != nil {
		return err
	}
	defer targetFile.Close()
	_, err = io.Copy(targetFile, reader)
	return err
}
