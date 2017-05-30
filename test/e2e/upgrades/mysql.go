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
	"k8s.io/kubernetes/pkg/util/version"
	"k8s.io/kubernetes/test/e2e/framework"
)

// StatefulSetUpgradeTest implements an upgrade test harness for StatefulSet upgrade testing.
type MySqlUpgradeTest struct {
	db *sql.DB
	success int
	failure int
}

func (MySqlUpgradeTest) Name() string { return "mysql-upgrade" }

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
	ns := f.Namespace.Name
	t.success = 0
	t.failure = 0
	mkpath := func(file string) string {
		return filepath.Join(framework.TestContext.RepoRoot, "test/e2e/testing-manifests", file)
	}

	By("Creating a configmap")
	configMapYaml := mkpath("configmap.yaml")
	framework.RunKubectlOrDie("create", "-f", configMapYaml, fmt.Sprintf("--namespace=%s", ns))

	By("Create services")
	servicesYaml := mkpath("services.yaml")
	framework.RunKubectlOrDie("create", "-f", servicesYaml, fmt.Sprintf("--namespace=%s", ns))

	By("Creating a mysql StatefulSet")
	ssYaml := mkpath("statefulset.yaml")
	framework.RunKubectlOrDie("create", "-f", ssYaml, fmt.Sprintf("--namespace=%s", ns))

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

	pod.ObjectMeta.Labels["writer"] = "writer"

	_, err = f.ClientSet.CoreV1().Pods(ns).Update(pod)
	Expect(err).NotTo(HaveOccurred())

	By("Waiting for the service's external IP to be available")
	// TODO(): use already implemented retry logic here. Just quick wrote this for temp fix.
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
			framework.Logf("Didn't get IP from service, waiting then trying again")
		}
		retries++
		if retries > 30 {
			keepGoing = false
		}
	}

	s := "root@tcp(" + ip + ":3306)/"

	By("Opening and connecting to the database using the writer service")
	db, err := sql.Open("mysql", s)
	Expect(err).NotTo(HaveOccurred())

	err = db.Ping()
	Expect(err).NotTo(HaveOccurred())

	By("Inserting some basic data into the database")

	res, err := db.Exec("CREATE DATABASE testing;")
	Expect(err).NotTo(HaveOccurred())
	framework.Logf("res was: %v", res)

	res, err = db.Exec("CREATE TABLE testing.users (name VARCHAR(150) NOT NULL, PRIMARY KEY (name));")
	Expect(err).NotTo(HaveOccurred())
	framework.Logf("res was: %v", res)

	res, err = db.Exec("INSERT INTO testing.users (name) VALUES(?);", "Ben")
	Expect(err).NotTo(HaveOccurred())
	framework.Logf("res was: %v", res)

	res, err = db.Exec("INSERT INTO testing.users (name) VALUES(?);", "Maisem")
	Expect(err).NotTo(HaveOccurred())
	framework.Logf("res was: %v", res)

	rows, err := db.Query("SELECT * FROM testing.users;")
	Expect(err).NotTo(HaveOccurred())
	framework.Logf("rows was: %v", rows)

	By("Making sure the input data is queryable")
	defer rows.Close()
	for rows.Next() {
		var user string
		err = rows.Scan(&user)
		Expect(err).NotTo(HaveOccurred())
		framework.Logf("Name from row was: %s", user)
	}
	err = rows.Err()
	Expect(err).NotTo(HaveOccurred())

	By("Closing the database")
	db.Close()

	By("Waiting for the read service's external ip to be available")
	retries = 0
	keepGoing = true
	for keepGoing {
		service, err := f.ClientSet.CoreV1().Services(ns).Get("mysql-read", metav1.GetOptions{})
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
			framework.Logf("Didn't get IP from service, waiting then trying again")
		}
		retries++
		if retries > 30 {
			keepGoing = false
		}
	}
	framework.Logf("ip is: %s", ip)
	s = "root@tcp(" + ip + ":3306)/"

	By("Opening and connecting to the database using the read service for use during the upgrade")
	db, err = sql.Open("mysql", s)
	Expect(err).NotTo(HaveOccurred())
	t.db = db

	err = t.db.Ping()
	Expect(err).NotTo(HaveOccurred())
}

// The test continually polls the database for the data that was inserted earlier
func (t *MySqlUpgradeTest) Test(f *framework.Framework, done <-chan struct{}, upgrade UpgradeType) {
	By("Continuously polling the database for inserted info")
	wait.Until(func() {
		rows, err := t.db.Query("SELECT * FROM testing.users")
		if err != nil {
			framework.Logf("Error while reading during test. Err: %v", err)
		}
		framework.Logf("During test, rows was: %v", rows)
		if rows != nil {
			for rows.Next() {
				var user string
				err = rows.Scan(&user)
				if err != nil {
					framework.Logf("Error while converting row into user during test. Err: %v", err)
				}
				framework.Logf("Name from row was: %s", user)
			}
			err = rows.Err()
			if err != nil {
				framework.Logf("Error after all rows. Err: %v", err)
			} else {
				t.success = t.success + 1
			}
		} else {
			t.failure = t.failure + 1
		}
	}, framework.Poll, done)
	framework.Logf("Success was: %d", t.success)
	framework.Logf("Failure was: %d", t.failure)
}

// Currently does nothing. Maybe should make sure the service still works, the data is still there, etc.
func (t *MySqlUpgradeTest) Teardown(f *framework.Framework) {
}
