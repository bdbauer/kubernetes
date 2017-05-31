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

// MySqlUpgradeTest implements an upgrade test harness for polling a replicated sql database.
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

// Setup creates a StatefulSet, HeadlessService, a Service to write to the db, and a Service to read
// from the db. It then connects to the db with the write Service and populates the db with a table
// and a few entries. Finally, it connects to the db with the read Service, and confirms the data is
// available. The read connection is left open to be used later in the test.
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

	By("Waiting for the StatefulSet's pods to be running")
	statefulsetPoll := 30 * time.Second
	statefulsetTimeout := 10 * time.Minute

	// numPets has to match the value from the SS. In future, could fetch value from file.
	numPets := 3
	// same with labels matching SS.
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

	By("Adding Labels[\"writer\"] = \"writer\" to pod mysql-0, used to connect with the write Service.")
	pod, err := f.ClientSet.CoreV1().Pods(ns).Get("mysql-0", metav1.GetOptions{})
	Expect(err).NotTo(HaveOccurred())

	pod.ObjectMeta.Labels["writer"] = "writer"

	_, err = f.ClientSet.CoreV1().Pods(ns).Update(pod)
	Expect(err).NotTo(HaveOccurred())

	By("Waiting for the write Service's external IP to be available.")
	// TODO(): use already implemented retry logic here. Just quick wrote this for temp fix.
	var ip string
	retries := 0
	keepGoing := true
	for keepGoing {
		service, err := f.ClientSet.CoreV1().Services(ns).Get("mysql-write", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		ingress := service.Status.LoadBalancer.Ingress
		if len(ingress) > 0 {
			ip = ingress[0].IP
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

	By("Connecting to the database using the write Service.")
	db, err := sql.Open("mysql", s)
	Expect(err).NotTo(HaveOccurred())

	err = db.Ping()
	Expect(err).NotTo(HaveOccurred())

	By("Inserting some basic data into the database.")

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

	db.Close()

	By("Waiting for the read Service's external IP to be available.")
	retries = 0
	keepGoing = true
	for keepGoing {
		service, err := f.ClientSet.CoreV1().Services(ns).Get("mysql-read", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		ingress := service.Status.LoadBalancer.Ingress
		if len(ingress) > 0 {
			ip = ingress[0].IP
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
	s = "root@tcp(" + ip + ":3306)/"

	By("Connecting to the database using the read service.")
	db, err = sql.Open("mysql", s)
	Expect(err).NotTo(HaveOccurred())
	t.db = db

	err = t.db.Ping()
	Expect(err).NotTo(HaveOccurred())

	By("Confirming the data is available before starting the upgrade.")
	err = t.CheckRows()
	Expect(err).NotTo(HaveOccurred())
}

// Test continually polls the db using the read Service connection, checking to see if the data
// inserted earlier is available.
func (t *MySqlUpgradeTest) Test(f *framework.Framework, done <-chan struct{}, upgrade UpgradeType) {
	By("Continuously polling the database during upgrade.")
	wait.Until(func() {
		err := t.CheckRows()
		if err != nil {
			framework.Logf("Error while trying to confirm data: %v", err)
			t.failure = t.failure + 1
		} else {
			t.success = t.success + 1
		}
	}, framework.Poll, done)

	framework.Logf("Success was: %d", t.success)
	framework.Logf("Failure was: %d", t.failure)
}

// Teardown does one final check of the data's availability.
func (t *MySqlUpgradeTest) Teardown(f *framework.Framework) {
	err := t.CheckRows()
	Expect(err).NotTo(HaveOccurred())
}

// Checks to make sure the values "Ben" and "Maisem" are in the table testing.users.
func (t *MySqlUpgradeTest) CheckRows() error {
	var user string

	rows, err := t.db.Query("SELECT * FROM testing.users;")
	if err != nil {
		return err
	}
	defer rows.Close()
	// Check first value, should be "Ben"
	if !rows.Next() {
		return fmt.Errorf("rows.Next() returned false. Expected another row to exist.")
	}
	err = rows.Scan(&user)
	if err != nil {
		return err
	}
	if user != "Ben" {
		return fmt.Errorf("Unexpected value reading from table. Got: %s, want: Ben.", user)
	}
	// Check second value, should be "Maisem"
	if !rows.Next() {
		return fmt.Errorf("rows.Next() returned false. Expected another row to exist.")
	}
	err = rows.Scan(&user)
	if err != nil {
		return err
	}
	if user != "Maisem" {
		return fmt.Errorf("Unexpected value reading from table. Got: %s, want: Maisem.", user)
	}
	return nil
}
