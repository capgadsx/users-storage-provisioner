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
)

func main() {
	var provisionerName string
	var dataDirectory string
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
	ldap            LDAPConfig
}

func (provisioner *CustomNFSUsersProvisioner) Provision(options controller.VolumeOptions) (*v1.PersistentVolume, error) {
	ownerUser, found := options.PVC.ObjectMeta.Annotations[provisioner.ownerAnnotation]
	if !found {
		return nil, errors.New(fmt.Sprintf("missing '%v' annotation", provisioner.ownerAnnotation))
	}
	userUID, userGID, err := GetUserGidUid(ownerUser, provisioner.ldap.server, provisioner.ldap.baseDN, provisioner.ldap.userFilter, provisioner.ldap.uidAttribute, provisioner.ldap.gidAttribute)
	if err != nil {
		return nil, err
	}
	glog.Infof("Creating new pv %v for user %v (uid: %v gid: %v)", options.PVName, ownerUser, userUID, userGID)
	customPVName := strings.Join([]string{"pv", ownerUser}, "-")
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
		os.OpenFile(filepath.Join(pvUserVolumePath, "USER_FILE1"), os.O_RDONLY|os.O_CREATE, 0600)
		os.OpenFile(filepath.Join(pvUserVolumePath, "USER_FILE2"), os.O_RDONLY|os.O_CREATE, 0600)
		os.Chown(filepath.Join(pvUserVolumePath, "USER_FILE1"), userUID, userGID)
		os.Chown(filepath.Join(pvUserVolumePath, "USER_FILE2"), userUID, userGID)
		os.OpenFile(pvSuccessFlagPath, os.O_RDONLY|os.O_CREATE, 0400)
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
