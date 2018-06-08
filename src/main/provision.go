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
)

func main() {
	flag.Parse()
	flag.Set("logtostderr", "true")
	//TODO: This variables should be flags
	provisionerName := "storage.cuda.labcomp.cl/users-provisioner"
	backendDirectory := "/persistentVolumesData"
	nfsServer := "nfs2.labcomp.cl"
	nfsPath := "/exports/cuda/users-pvs"
	ownerAnnotation := "storage.cuda.labcomp.cl/owner"
	archivedPVPrefix := "deleted"
	//END
	glog.Info("Starting custom dynamic pv provisioner")
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
		backendDirectory: backendDirectory,
		server:           nfsServer,
		path:             nfsPath,
		ownerAnnotation:  ownerAnnotation,
		archivedPVPrefix: archivedPVPrefix,
	}
	provisionController := controller.NewProvisionController(clientSet, provisionerName, provisioner, serverVersion.GitVersion)
	provisionController.Run(wait.NeverStop)
}

type CustomNFSUsersProvisioner struct {
	backendDirectory string
	server           string
	path             string
	ownerAnnotation  string
	archivedPVPrefix string
}

func (provisioner *CustomNFSUsersProvisioner) Provision(options controller.VolumeOptions) (*v1.PersistentVolume, error) {
	glog.Infof("Creating new pv %v", options.PVName)
	ownerUser, found := options.PVC.ObjectMeta.Annotations[provisioner.ownerAnnotation]
	if !found {
		return nil, errors.New(fmt.Sprintf("missing '%v' annotation", provisioner.ownerAnnotation))
	}
	customPVName := strings.Join([]string{"pv", ownerUser}, "-")
	pvPath := filepath.Join(provisioner.backendDirectory, customPVName)
	glog.Infof("Creating path %v", pvPath)
	if err := os.MkdirAll(pvPath, 0740); err != nil {
		return nil, errors.New(fmt.Sprintf("failed to create directory %v (caused by %v)", pvPath, err))
	}
	os.OpenFile(filepath.Join(pvPath, ".cuda_success"), os.O_RDONLY|os.O_CREATE, 0666)
	os.Chmod(pvPath, 0740)
	mountPath := filepath.Join(provisioner.path, customPVName)
	glog.Infof("NFS path for new PVSource: %v:%v", provisioner.server, mountPath)
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
	glog.Infof("Deleting pv %v", volume.Name)
	path := volume.Spec.PersistentVolumeSource.NFS.Path
	pvName := filepath.Base(path)
	oldPath := filepath.Join(provisioner.backendDirectory, pvName)
	archivedPath := filepath.Join(provisioner.backendDirectory, provisioner.archivedPVPrefix+"-"+pvName)
	glog.Infof("Archiving path %v to %v", oldPath, archivedPath)
	return os.Rename(oldPath, archivedPath)
}
