package main

import (
	"time"
	"flag"
	"log"
	"encoding/json"
	"os"
	"os/signal"
	"syscall"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/api/apps/v1"
	"github.com/ghodss/yaml"
)

const(
	defaultInitializerName = "mpp.initializer.kubernetes.io"
	defaultConfigmap = "mpp-initializer"
)

var(
	initializerName string
	configmap string
	namespace string
)

func main(){
	flag.StringVar(&initializerName, "initializer-name", defaultInitializerName, "")
	flag.StringVar(&configmap, "configmap", defaultConfigmap, "")
	flag.StringVar(&namespace, "namespace", "default", "")
	flag.Parse()
	clusterConfig, err := clientcmd.BuildConfigFromFlags("", "/root/.kube/config")
	if err != nil{
		log.Fatalln(err.Error())
		return
	}
	
	clientset, err := kubernetes.NewForConfig(clusterConfig)
	if err != nil{
		log.Fatalln(err.Error())
		return
	}
	
	cm, err := clientset.CoreV1().ConfigMaps(namespace).Get(configmap, metav1.GetOptions{})
	term, err := configmapToTerm(cm)
	if err != nil{
		log.Fatalln(err.Error())
		return
	}
	
	restclient := clientset.AppsV1().RESTClient()
	listWatch := cache.NewListWatchFromClient(restclient, "statefulsets", corev1.NamespaceAll, fields.Everything())
	includeUninitializedWatchList := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error){
			options.IncludeUninitialized = true
			return listWatch.List(options)
		},
		
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error){
			options.IncludeUninitialized = true
			return listWatch.Watch(options)
		},
	}
	
	resyncPeriod := 30 * time.Second
	_, ssInformer := cache.NewInformer(includeUninitializedWatchList, &v1.StatefulSet{}, resyncPeriod, 
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(object interface{}) {
				err := initializeStatefulset(object.(*v1.StatefulSet), term, clientset)
				if err != nil{
					log.Fatalln(err.Error())
				}
			},
		},
	)
	
	stopCh := make(chan struct{})
	go ssInformer.Run(stopCh)
	
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)
	<-signalCh
	log.Println("Shutdown signal received, exiting...")
	
	close(stopCh)
}

func initializeStatefulset(ss *v1.StatefulSet, term corev1.PodAffinityTerm, clientset *kubernetes.Clientset) error{
	if ss.ObjectMeta.GetInitializers() != nil{
		pendingInitializers := ss.ObjectMeta.GetInitializers().Pending
		
		if initializerName == pendingInitializers[0].Name{
			log.Printf("initializing statefulset %s\n", ss.Name)
		}
		initializedSS := new(v1.StatefulSet)
		ss.DeepCopyInto(initializedSS)
		initializedSS.ObjectMeta.Initializers = nil
		
		initializedSS.Spec.Template.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = append(initializedSS.Spec.Template.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution, term)
		
		oldData, err := json.Marshal(ss)
		if err != nil{
			return err
		}
		
		newData, err := json.Marshal(initializedSS)
		if err != nil{
			return err
		}
		
		patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, v1.StatefulSet{})
		if err != nil{
			return err
		}
		
		_, err = clientset.AppsV1().StatefulSets(ss.Namespace).Patch(ss.Name, types.MergePatchType, patchBytes)
		if err != nil{
			return err
		}
	}
	
	return nil
}

func configmapToTerm(configmap *corev1.ConfigMap) (corev1.PodAffinityTerm, error){
	var term corev1.PodAffinityTerm
	err := yaml.Unmarshal([]byte(configmap.Data["config"]), &term)
	if err != nil{
		log.Fatalln(err.Error())
	}
	
	return term, err
}