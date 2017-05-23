/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package upgrades

import (
	"fmt"
	"time"
	"path/filepath"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/pkg/api/v1"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	//apps "k8s.io/kubernetes/pkg/apis/apps/v1beta1"
	"k8s.io/kubernetes/pkg/util/version"
	"k8s.io/kubernetes/test/e2e/framework"
)

// StatefulSetUpgradeTest implements an upgrade test harness for StatefulSet upgrade testing.
type MySqlUpgradeTest struct {
	//tester  *framework.StatefulSetTester
	//service *v1.Service
	//set     *apps.StatefulSet
	db *sql.DB
}

func (MySqlUpgradeTest) Name() string { return "postgres-upgrade" }

func (MySqlUpgradeTest) Skip(upgCtx UpgradeContext) bool {
	minVersion := version.MustParseSemantic("1.4.0")

	for _, vCtx := range upgCtx.Versions {
		if vCtx.Version.LessThan(minVersion) {
			return true
		}
	}
	return false
}

// Setup creates a StatefulSet and a HeadlessService. It verifies the basic SatefulSet properties
func (t *MySqlUpgradeTest) Setup(f *framework.Framework) {
	/*
	ssName := "ss"
	labels := map[string]string{
		"foo": "bar",
		"baz": "blah",
	}
	*/


	// Set up Volumes?
	// Create config map
	// Create Both Services
	// CreateStatefulSet

	// Create database / table
	// Insert some data
	// Read data
	// Start disruption
	// Continue reading data
	// End disruption
	// Read data



	//t.tester = framework.NewStatefulSetTester(f.ClientSet)
	ns := f.Namespace.Name

	/*
	pv1 := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pv1",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: v1.PersistentVolumeReclaimDelete,
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse("10Gi"),
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				HostPath: &v1.HostPathVolumeSource{Path: "/data/1"},
			},
		},
	}
	pv1, err := f.ClientSet.Core().PersistentVolumes().Create(pv1)
	Expect(err).NotTo(HaveOccurred())

	pv2 := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pv2",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: v1.PersistentVolumeReclaimDelete,
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse("10Gi"),
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				HostPath: &v1.HostPathVolumeSource{Path: "/data/2"},
			},
		},
	}
	pv2, err = f.ClientSet.Core().PersistentVolumes().Create(pv2)
	Expect(err).NotTo(HaveOccurred())

	pv3 := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pv3",
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: v1.PersistentVolumeReclaimDelete,
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): resource.MustParse("10Gi"),
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				HostPath: &v1.HostPathVolumeSource{Path: "/data/3"},
			},
		},
	}
	pv3, err = f.ClientSet.Core().PersistentVolumes().Create(pv3)
	Expect(err).NotTo(HaveOccurred())
	*/
	mkpath := func(file string) string {
		return filepath.Join(framework.TestContext.RepoRoot, "test/e2e/testing-manifests", file)
	}

	By("Creating a configmap")
	configMapYaml := mkpath("configmap.yaml")
	framework.RunKubectlOrDie("create", "-f", configMapYaml, fmt.Sprintf("--namespace=%s", ns))
	// Maybe wait for map to create?

	By("Create services to access resources")
	servicesYaml := mkpath("services.yaml")
	framework.RunKubectlOrDie("create", "-f", servicesYaml, fmt.Sprintf("--namespace=%s", ns))
	// Maybe wait for services to create?

	By("Creating a mysql StatefulSet")
	ssYaml := mkpath("statefulset.yaml")
	framework.RunKubectlOrDie("create", "-f", ssYaml, fmt.Sprintf("--namespace=%s", ns))


	//nsFlag := fmt.Sprintf("--namespace=%v", ns)

	//time.Sleep(time.Minute * 5)

	By("Waiting for the statefulset's pods to be running")

	statefulsetPoll := 30 * time.Second
	statefulsetTimeout := 10 * time.Minute
	// Maybe get next values straight from yaml instead? dono how
	numPets := 3
	label := labels.SelectorFromSet(labels.Set(map[string]string{"app": "mysql"}))

	err := wait.PollImmediate(statefulsetPoll, statefulsetTimeout,
		func() (bool, error) {
			podList, err := f.ClientSet.CoreV1().Pods(ns).List(metav1.ListOptions{LabelSelector: label.String()})
			if err != nil {
				return false, fmt.Errorf("Unable to get list of pods in statefulset %s", label)
			}
			if len(podList.Items) < numPets {
				framework.Logf("Found %d pets, waiting for %d", len(podList.Items), numPets)
				return false, nil
			}
			if len(podList.Items) > numPets {
				return false, fmt.Errorf("Too many pods scheduled, expected %d got %d", numPets, len(podList.Items))
			}
			for _, p := range podList.Items {
				isReady := podutil.IsPodReady(&p)
				if p.Status.Phase != v1.PodRunning || !isReady {
					framework.Logf("Waiting for pod %v to enter %v - Ready=True, currently %v - Ready=%v", p.Name, v1.PodRunning, p.Status.Phase, isReady)
					return false, nil
				}
			}
			return true, nil
		})
	Expect(err).NotTo(HaveOccurred())


	By("Adding the writer=writer label to mysql-0")

	pod, err := f.ClientSet.CoreV1().Pods(ns).Get("mysql-0", metav1.GetOptions{})
	Expect(err).NotTo(HaveOccurred())

	framework.Logf("POD WAS: %v", pod)

	// add label
	pod.ObjectMeta.Labels["writer"] = "writer"

	pod, err = f.ClientSet.CoreV1().Pods(ns).Update(pod)
	Expect(err).NotTo(HaveOccurred())

	framework.Logf("POD AFTER UPDATE WAS: %v", pod)

	/*
	output := framework.RunKubectlOrDie("run", "mysql-client", "--image=mysql:5.7", "-i", "-t", "--rm", "--restart=Never", "--", "mysql", "-h", "mysql-0.mysql", "-e", "CREATE DATABASE testing;")
	By("Seeing what output is: " + output)
	time.Sleep(5 * time.Second)

	output = framework.RunKubectlOrDie("run", "mysql-client", "--image=mysql:5.7", "-i", "-t", "--rm", "--restart=Never", "--", "mysql", "-h", "mysql-0.mysql", "-e", "CREATE TABLE testing.users (name VARCHAR(150) NOT NULL, PRIMARY KEY (name));")
	By("Seeing what output is: " + output)
	time.Sleep(5 * time.Second)

	output = framework.RunKubectlOrDie("run", "mysql-client", "--image=mysql:5.7", "-i", "-t", "--rm", "--restart=Never", "--", "mysql", "-h", "mysql-0.mysql", "-e", "INSERT INTO testing.users (name) VALUES ('Ben');")
	By("Seeing what output is: " + output)
	time.Sleep(5 * time.Second)

	output = framework.RunKubectlOrDie("run", "mysql-client", "--image=mysql:5.7", "-i", "-t", "--rm", "--restart=Never", "--", "mysql", "-h", "mysql-read", "-e", "SELECT * FROM testing.users;")
	By("Seeing what output is: " + output)
	time.Sleep(5 * time.Second)

	*/



	var ip string
	retries := 0
	keepGoing := true
	for keepGoing {
		service, err := f.ClientSet.CoreV1().Services(ns).Get("mysql-write", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		framework.Logf("Service was: %v", service)
		ingress := service.Status.LoadBalancer.Ingress
		framework.Logf("Ingress was: %v", ingress)
		if len(ingress) > 0 {
			ip = ingress[0].IP
			framework.Logf("Ip was: %v", ip)
			retries = 30
		} else {
			time.Sleep(time.Second * 20)
		}
		framework.Logf("Didn't get IP from service, waiting then trying again")
		retries++
		if retries > 30 {
			keepGoing = false
		}
	}
	framework.Logf("ip is: %s", ip)
	s := "root:@tcp([" + ip + "]:3306)/"


	By("Opening and connecting to the database")
	db, err := sql.Open("mysql", s)
	Expect(err).NotTo(HaveOccurred())
	//t.db = db
	//defer db.Close()

	err = db.Ping()
	Expect(err).NotTo(HaveOccurred())

	By("Inserting some basic data into the database")

	stmt, err := db.Prepare("CREATE DATABASE testing")
	Expect(err).NotTo(HaveOccurred())
	framework.Logf("stmt was: %v", stmt)

	res, err := stmt.Exec()
	Expect(err).NotTo(HaveOccurred())
	framework.Logf("res was: %v", res)

	stmt, err = db.Prepare("CREATE TABLE testing.users (name VARCHAR(150) NOT NULL, PRIMARY KEY (name))")
	Expect(err).NotTo(HaveOccurred())
	framework.Logf("stmt was: %v", stmt)

	res, err = stmt.Exec()
	Expect(err).NotTo(HaveOccurred())
	framework.Logf("res was: %v", res)

	stmt, err = db.Prepare("INSERT INTO testing.users (name) VALUES(?)")
	Expect(err).NotTo(HaveOccurred())
	framework.Logf("stmt was: %v", stmt)

	res, err = stmt.Exec("Ben")
	Expect(err).NotTo(HaveOccurred())
	framework.Logf("res was: %v", res)

	res, err = stmt.Exec("Maisem")
	Expect(err).NotTo(HaveOccurred())
	framework.Logf("res was: %v", res)

	rows, err := db.Query("SELECT * FROM testing.users")
	Expect(err).NotTo(HaveOccurred())
	framework.Logf("rows was: %v", rows)





	//stmt, err := db.Prepare("CREATE TABLE users ")
	// check err

	/*
	manifestPath := "./yamls/"
	mkpath := func(file string) string {
		return filepath.Join(manifestPath, file)
	}
	pvYaml := generated.ReadOrDie(mkpath("storage.yaml"))
	framework.RunKubectlOrDieInput(string(pvYaml[:]), "create", "-f", "-", fmt.Sprintf("--namespace=%v", ns))

	*/
	/*

	svcName := "test"
	statefulPodMounts := []v1.VolumeMount{{Name: "datadir", MountPath: "/data/"}}
	ns := f.Namespace.Name

	By("Creating the stateful set")
	t.set = CreateStatefulSet(ssName, ns, svcName, 2, statefulPodMounts, labels)
	// t.set = framework.NewStatefulSet(ssName, ns, headlessSvcName, 2, statefulPodMounts, podMounts, labels)

	By("Creating the stateful service")
	t.service = framework.CreateStatefulSetService(ssName, labels)
	// *(t.set.Spec.Replicas) = 3
	//
	By("Setting the initialized anotation")
	framework.SetStatefulSetInitializedAnnotation(t.set, "false")

	By("Creating service " + svcName + " in namespace " + ns)
	_, err := f.ClientSet.Core().Services(ns).Create(t.service)
	Expect(err).NotTo(HaveOccurred())
	t.tester = framework.NewStatefulSetTester(f.ClientSet)

	By("Creating statefulset " + ssName + " in namespace " + ns)
	// *(t.set.Spec.Replicas) = 3
	t.set, err = f.ClientSet.Apps().StatefulSets(ns).Create(t.set)
	Expect(err).NotTo(HaveOccurred())

	By("Saturating stateful set " + t.set.Name)
	t.tester.Saturate(t.set)
	t.verify()
	t.restart()
	t.verify()
	*/
}



/*
func CreateStatefulSet(name, ns, svcName string, replicas int32, mounts []v1.VolumeMount, labels map[string]string) *apps.StatefulSet {
	claims := []v1.PersistentVolumeClaim{}
	for _, m := range mounts {
		claims = append(claims, framework.NewStatefulSetPVC(m.Name))
	}

	return &apps.StatefulSet{
		TypeMeta: metav1.TypeMeta{
			Kind: "StatefulSet",
			APIVersion: "apps/v1beta1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Namespace: ns,
		},
		Spec: apps.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: labels,
			},
			Replicas: func(i int32) *int32 { return &i }(replicas),
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
					Annotations: map[string]string{},
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "pg",
							Image: "postgres",
							VolumeMounts: mounts,
						},
					},
					// Maybe add volumes here
				},
			},
			VolumeClaimTemplates: claims,
			ServiceName: svcName,
		},
	}
}
*/

// Waits for the upgrade to complete and verifies the StatefulSet basic functionality
func (t *MySqlUpgradeTest) Test(f *framework.Framework, done <-chan struct{}, upgrade UpgradeType) {
	By("Continuously polling the database for inserted info")
	wait.Until(func() {
		//output, err := framework.RunKubectl("run", "mysql-client", "--image=mysql:5.7", "-i", "-t", "--rm", "--restart=Never", "--", "mysql", "-h", "mysql-read", "-e", "SELECT * FROM testing.users;")
		//framework.Logf("Output was: %q", output)
		//framework.Logf("Error was : %v", err)


		//TODO: do polling here somehow.
	}, framework.Poll, done)
}

// Deletes all StatefulSets
func (t *MySqlUpgradeTest) Teardown(f *framework.Framework) {
	//framework.DeleteAllStatefulSets(f.ClientSet, t.set.Name)
	//output := framework.RunKubectlOrDie("run", "mysql-client", "--image=mysql:5.7", "-i", "-t", "--rm", "--restart=Never", "--", "mysql", "-h", "mysql-read", "-e", "SELECT * FROM testing.users;")
	//framework.Logf("Output was: %q", output)
}
/*
func (t *PostgresUpgradeTest) verify() {
	By("Verifying statefulset mounted data directory is usable")
	framework.ExpectNoError(t.tester.CheckMount(t.set, "/data"))

	By("Verifying statefulset provides a stable hostname for each pod")
	framework.ExpectNoError(t.tester.CheckHostname(t.set))

	By("Verifying statefulset set proper service name")
	framework.ExpectNoError(t.tester.CheckServiceName(t.set, t.set.Spec.ServiceName))

	cmd := "echo $(hostname) > /data/hostname; sync;"
	By("Running " + cmd + " in all stateful pods")
	framework.ExpectNoError(t.tester.ExecInStatefulPods(t.set, cmd))
}

func (t *PostgresUpgradeTest) restart() {
	By("Restarting statefulset " + t.set.Name)
	t.tester.Restart(t.set)
	t.tester.Saturate(t.set)
}
*/
