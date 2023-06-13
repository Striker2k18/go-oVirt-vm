package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"

	ovirtsdk4 "github.com/ovirt/go-ovirt"
)

type VMParams struct {
	Name             string
	Template         string
	Cluster          string
	Class            string
	Nic              string
	IP               string
	Gateway          string
	Mask             string
	DNS              string
	DNS1             string
	DNS2             string
	CPUCores         int
	CPUSockets       int
	Memory           int64
	MemoryGuaranteed int64
	Size             int64
}

func parseCSV(filename string) ([]VMParams, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer f.Close()

	r := csv.NewReader(f)
	var vms []VMParams
	line := 1 // Track line number for error reporting
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read CSV record at line %d: %w", line, err)
		}

		if len(record) != 16 {
			return nil, fmt.Errorf("invalid number of fields in CSV record at line %d", line)
		}

		cpuCores, err := strconv.Atoi(record[11])
		if err != nil {
			return nil, fmt.Errorf("failed to parse CPU cores at line %d: %w", line, err)
		}

		cpuSockets, err := strconv.Atoi(record[12])
		if err != nil {
			return nil, fmt.Errorf("failed to parse CPU sockets at line %d: %w", line, err)
		}

		memory, err := strconv.ParseInt(record[13], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse memory at line %d: %w", line, err)
		}

		memoryGuaranteed, err := strconv.ParseInt(record[14], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse guaranteed memory at line %d: %w", line, err)
		}

		size, err := strconv.ParseInt(record[15], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("failed to parse disk size at line %d: %w", line, err)
		}

		vm := VMParams{
			Name:             record[0],
			Template:         record[1],
			Cluster:          record[2],
			Class:            record[3],
			Nic:              record[4],
			IP:               record[5],
			Gateway:          record[6],
			Mask:             record[7],
			DNS:              record[8],
			DNS1:             record[9],
			DNS2:             record[10],
			CPUCores:         cpuCores,
			CPUSockets:       cpuSockets,
			Memory:           memory,
			MemoryGuaranteed: memoryGuaranteed,
			Size:             size,
		}
		vms = append(vms, vm)

		line++
	}
	return vms, nil
}

func createVM(vmParams VMParams, conn *ovirtsdk4.Connection, wg *sync.WaitGroup, errors chan error) {
	defer wg.Done()

	vmsService := conn.SystemService().VmsService()

	// Retrieve the template information
	templateName := vmParams.Template
	templateService := conn.SystemService().TemplatesService()
	templateResponse, err := templateService.List().Search("name=" + templateName).Send()
	if err != nil {
		errors <- fmt.Errorf("failed to retrieve template %s: %w", templateName, err)
		return
	}

	templates := templateResponse.MustTemplates().Slice()
	if len(templates) == 0 {
		errors <- fmt.Errorf("template %s not found", templateName)
		return
	}

	template := templates[0]

	// Retrieve the disk and VNIC names from the template
	diskName := template.MustVm().MustDisks().Slice()[0].MustName()
	vnicName := template.MustVm().MustNics().Slice()[0].MustName()

	vmBuilder := ovirtsdk4.NewVmBuilder()
	vmBuilder.Name(vmParams.Name)
	vmBuilder.ClusterBuilder(ovirtsdk4.NewClusterBuilder().Name(vmParams.Cluster))
	vmBuilder.TemplateBuilder(ovirtsdk4.NewTemplateBuilder().Name(templateName))
	vmBuilder.CpuBuilder(ovirtsdk4.NewCpuBuilder().TopologyBuilder(ovirtsdk4.NewCpuTopologyBuilder().Cores(int64(vmParams.CPUCores)).Sockets(int64(vmParams.CPUSockets))))
	vmBuilder.Memory(vmParams.Memory)
	vmBuilder.MemoryPolicyBuilder(ovirtsdk4.NewMemoryPolicyBuilder().Guaranteed(vmParams.MemoryGuaranteed))

	diskBuilder := ovirtsdk4.NewDiskBuilder()
	diskBuilder.Name(diskName)
	diskBuilder.ProvisionedSize(vmParams.Size)
	diskBuilder.Format(ovirtsdk4.DISKFORMAT_COW)
	diskBuilder.Sparse(true)
	diskBuilder.StorageDomainsBuilder(
		ovirtsdk4.NewStorageDomainBuilder().Name("my_storage_domain"),
	)

	vmBuilder.DiskAttachmentsBuilder(
		ovirtsdk4.NewDiskAttachmentBuilder().DiskBuilder(diskBuilder).Interface(ovirtsdk4.DISKINTERFACE_VIRTIO),
	)

	nicBuilder := ovirtsdk4.NewNicBuilder()
	nicBuilder.Name(vnicName)
	nicBuilder.Interface(ovirtsdk4.NICINTERFACE_VIRTIO)
	nicBuilder.VnicProfileBuilder(
		ovirtsdk4.NewVnicProfileBuilder().Name("my_network"),
	)

	vmBuilder.NicsBuilder(nicBuilder)

	vmBuilder.InitializationBuilder(
		ovirtsdk4.NewInitializationBuilder().
			CustomScript(fmt.Sprintf(`#cloud-config
			networking:
			  version: 1
			  config:
			  - type: physical
			    name: %s
			    subnets:
			    - type: static
			      address: %s
			      netmask: %s
			      gateway: %s
			  dns_nameservers:
			  - %s
			  - %s
			  - %s`, vmParams.Nic, vmParams.IP, vmParams.Mask, vmParams.Gateway, vmParams.DNS, vmParams.DNS1, vmParams.DNS2)),
	)

	resp, err := vmsService.Add().Vm(vmBuilder.MustBuild()).Send()
	if err != nil {
		errors <- fmt.Errorf("failed to create VM %s: %w", vmParams.Name, err)
		return
	}

	vmID := resp.MustVm().MustId()
	log.Printf("VM %s created successfully with ID: %s", vmParams.Name, vmID)

	vmService := vmsService.Vm(vmID)
	_, err = vmService.Start().MustSend()
	if err != nil {
		errors <- fmt.Errorf("failed to start VM %s: %w", vmParams.Name, err)
		return
	}

	log.Printf("VM %s started successfully", vmParams.Name)
}

func main() {
	csvFile := flag.String("csv", "vm_params.csv", "CSV file containing VM parameters")
	ovirtURL := flag.String("url", "https://your.ovirt.engine/ovirt-engine/api", "oVirt engine URL")
	username := flag.String("username", "your-username", "oVirt username")
	password := flag.String("password", "your-password", "oVirt password")
	insecure := flag.Bool("insecure", true, "Skip SSL certificate verification")
	concurrency := flag.Int("concurrency", 5, "Number of concurrent VM creations")

	flag.Parse()

	vms, err := parseCSV(*csvFile)
	if err != nil {
		log.Fatalf("Failed to parse CSV file: %v", err)
	}

	conn, err := ovirtsdk4.NewConnectionBuilder().
		URL(*ovirtURL).
		Username(*username).
		Password(*password).
		Insecure(*insecure).
		Build()
	if err != nil {
		log.Fatalf("Failed to create connection to the oVirt engine: %v", err)
	}
	defer conn.Close()

	var wg sync.WaitGroup
	errors := make(chan error, len(vms))
	semaphore := make(chan struct{}, *concurrency)

	for i := 0; i < len(vms); i++ {
		wg.Add(1)
		go func(vmParams VMParams) {
			semaphore <- struct{}{} // Acquire semaphore slot
			defer func() {
				<-semaphore // Release semaphore slot
			}()
			createVM(vmParams, conn, &wg, errors)
		}(vms[i])
	}

	wg.Wait()
	close(errors)

	for err := range errors {
		log.Println(err)
	}
}
