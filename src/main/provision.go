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
	var provisionerName string
	var dataDirectory string
	var nfsServer string
	var nfsPath string
	var ownerAnnotation string
	flag.StringVar(&provisionerName, "name", "storage.example.com/custom", "The name of this provisioner")
	flag.StringVar(&dataDirectory, "data", "/data", "Path were pv's are created inside the container")
	flag.StringVar(&nfsServer, "server", "127.0.0.1", "NFS Server were pv's are stored ")
	flag.StringVar(&nfsPath, "path", "/exports/pvs", "NFS Path were pv's are stored")
	flag.StringVar(&ownerAnnotation, "ann", "storage.example.com/owner", "Annotation used to identify owner user of the provisioned pv")
	flag.Parse()
	flag.Set("logtostderr", "true")
	glog.Info("Starting custom dynamic pv provisioner")
	glog.Infof("Flags:")
	glog.Infof("		-name: %v", provisionerName)
	glog.Infof("		-data: %v", dataDirectory)
	glog.Infof("		-server: %v", nfsServer)
	glog.Infof("		-path: %v", nfsPath)
	glog.Infof("		-ann: %v", ownerAnnotation)
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
	}
	provisionController := controller.NewProvisionController(clientSet, provisionerName, provisioner, serverVersion.GitVersion)
	provisionController.Run(wait.NeverStop)
}

type CustomNFSUsersProvisioner struct {
	dataDirectory   string
	server          string
	path            string
	ownerAnnotation string
}

func (provisioner *CustomNFSUsersProvisioner) Provision(options controller.VolumeOptions) (*v1.PersistentVolume, error) {
	ownerUser, found := options.PVC.ObjectMeta.Annotations[provisioner.ownerAnnotation]
	if !found {
		return nil, errors.New(fmt.Sprintf("missing '%v' annotation", provisioner.ownerAnnotation))
	}
	glog.Infof("Creating new pv %v for user %v", options.PVName, ownerUser)
	customPVName := strings.Join([]string{"pv", ownerUser}, "-")
	pvPath := filepath.Join(provisioner.dataDirectory, customPVName)
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
	glog.Infof("Deleting pv %v from database", volume.Name)
	//Since this contains the user home, we don't delete the files..
	return nil
}
