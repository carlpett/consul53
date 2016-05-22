package main

import (
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/route53"
	consul "github.com/hashicorp/consul/api"
	log "github.com/sirupsen/logrus"
)

/*
TODO:
	- Logging
	- Batch updates (wait x ms after update to group)
	- Swap aws-sdk-go for mitchellh/goamz
	- Implement leader-election
	- Better commandline lib
	- Add marker TXT for X-Consul-Index?
	- Healthcheck that R53 reachable?
	- Multiple zones/different zones per service (will probably require config file?)
*/

type serviceList []string

var (
	consulAddress string
	zone          string
	services      serviceList
	logLevel      log.Level = log.InfoLevel
)

func (i *serviceList) String() string {
	return "List of services" // TODO: Comma-join list?
}

func (i *serviceList) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func init() {
	var rawLevel string
	flag.StringVar(&consulAddress, "consul", "localhost:8500", "consul ip:port")
	flag.StringVar(&zone, "zone", "", "route53 zone id")
	flag.StringVar(&rawLevel, "log-level", "info", "log level")
	flag.Var(&services, "service", "service to sync")

	flag.Parse()

	parsedLevel, err := log.ParseLevel(rawLevel)
	if err != nil {
		fmt.Printf("Could not parse log level: %q\n", err)
		os.Exit(1)
	}
	logLevel = parsedLevel
	log.SetLevel(logLevel)

	if zone == "" {
		fmt.Println("Parameter -zone is required")
		os.Exit(1)
	}
	if len(services) == 0 {
		fmt.Println("Must specify at least one service")
		os.Exit(1)
	}
}

func main() {
	client, err := consul.NewClient(consul.DefaultConfig())
	if err != nil {
		panic(err)
	}
	catalog := client.Catalog()

	dnsClient := route53.New(session.New(), &aws.Config{})

	// TODO: Wait to aquire lock - OR should that just be on writes? Always watch, but only write if master?

	updates := make(chan serviceResouce)
	for _, service := range services {
		go watchService(catalog, service, updates)
	}
	for {
		select {
		case u := <-updates:
			log.Debug("Service update: %v\n", u)
			updateRoute53(dnsClient, u)
		}
	}
}

type serviceResouce struct {
	name      string
	addresses []string
}

func updateRoute53(serviceClient *route53.Route53, update serviceResouce) {
	records := make([]*route53.ResourceRecord, len(update.addresses))
	for idx, ip := range update.addresses {
		records[idx] = &route53.ResourceRecord{
			Value: aws.String(ip),
		}
	}
	t := time.Now()
	change := &route53.ChangeResourceRecordSetsInput{
		ChangeBatch: &route53.ChangeBatch{
			Changes: []*route53.Change{
				{
					Action: aws.String("UPSERT"),
					ResourceRecordSet: &route53.ResourceRecordSet{
						Name:            aws.String(update.name),
						Type:            aws.String("A"), // TODO: Check for ipv6! Would require splitting into multiple changes
						ResourceRecords: records,
						TTL:             aws.Int64(15),
					},
				},
			},
			Comment: aws.String(fmt.Sprintf("Updated by consul53 at %v", t.Format(time.RFC3339))), // TODO
		},
		HostedZoneId: aws.String(zone),
	}
	fmt.Println(change)
	//resp, err := serviceClient.ChangeResourceRecordSets(params)
}

func watchService(catalog *consul.Catalog, service string, updates chan serviceResouce) {
	logger := log.WithFields(log.Fields{"service": service})
	serviceData, meta, err := catalog.Service(service, "", nil)
	if err != nil {
		logger.Error("Error while querying service catalog for initial state", err)
		panic(err)
	}
	serviceIPs := getServiceIPs(serviceData)
	logger.Debug("Stared service watcher")

	lastIndex := meta.LastIndex
	queryOpts := consul.QueryOptions{WaitIndex: lastIndex}
	for {
		serviceData, meta, err := catalog.Service(service, "", &queryOpts)
		if err != nil {
			logger.Error("Error while querying service catalog", err)
			continue
		}
		queryOpts.WaitIndex = meta.LastIndex

		latest := getServiceIPs(serviceData)
		if len(latest) == 0 {
			logger.Warn("No nodes left on service! Ignoring update")
			continue
		}
		if ipsEqual(serviceIPs, latest) {
			logger.Debug("No-op change, service unchanged")
			continue
		}

		serviceIPs = latest
		updates <- serviceResouce{name: service, addresses: serviceIPs}
	}
}

func getServiceIPs(serviceData []*consul.CatalogService) []string {
	ips := make([]string, len(serviceData))
	for idx, s := range serviceData {
		if s.ServiceAddress != "" {
			ips[idx] = s.ServiceAddress
		} else {
			ips[idx] = s.Address
		}
	}
	sort.Strings(ips)
	return ips
}
func ipsEqual(lhs, rhs []string) bool {
	if len(lhs) != len(rhs) {
		return false
	}
	for idx := range lhs {
		if lhs[idx] != rhs[idx] {
			return false
		}
	}
	return true
}
