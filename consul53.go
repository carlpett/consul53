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
	minWait       time.Duration
	maxWait       time.Duration
	dryRun        bool
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
	flag.StringVar(&zone, "zone", "", "route53 zone id")
	flag.Var(&services, "service", "service to sync")
	flag.StringVar(&consulAddress, "consul", "localhost:8500", "consul ip:port")
	flag.DurationVar(&minWait, "min-wait", 500*time.Millisecond, "minimum quiet time before syncing pending changes")
	flag.DurationVar(&maxWait, "max-wait", 5*time.Second, "maximum time a change can be pending")
	flag.StringVar(&rawLevel, "log-level", "info", "log level")
	flag.BoolVar(&dryRun, "dry-run", false, "if true, do not sync changes to route 53, output to stdout")

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

	if minWait >= maxWait {
		fmt.Println("max-wait must be longer than min-wait")
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

	updates := make(chan serviceResourceChange)
	for _, service := range services {
		go watchService(catalog, service, updates)
	}

	pending := make(map[string]serviceResourceChange)
	for {
		select {
		case u := <-updates:
			log.WithFields(log.Fields{"service": u.name, "addresses": u.addresses}).Debug("Service updated")
			pending[u.name] = u

		case <-time.After(maxWait - maxAge(&pending)):
			log.WithFields(log.Fields{"pending": pending, "max-wait": maxWait}).Debug("At least one change is older than max-wait threshold. Syncing.")
			updateRoute53(dnsClient, &pending)
			clearPendingChanges(&pending)

		case <-time.After(minWait):
			log.Debug("Sync window expired, checking pending changes")
			if len(pending) > 0 {
				log.WithFields(log.Fields{"pending": pending, "min-wait": minWait}).Debug("Pending changes and no updates during min-wait threshold. Syncing.")
				updateRoute53(dnsClient, &pending)
				clearPendingChanges(&pending)
			}
		}
	}
}

func maxAge(changes *map[string]serviceResourceChange) time.Duration {
	d := 0 * time.Millisecond
	for _, v := range *changes {
		if age := time.Since(v.timestamp); age > d {
			d = age
		}
	}
	return d
}
func clearPendingChanges(changes *map[string]serviceResourceChange) {
	for k := range *changes {
		delete(*changes, k)
	}
}

type serviceResourceChange struct {
	name      string
	addresses []string
	timestamp time.Time
}

func updateRoute53(serviceClient *route53.Route53, updates *map[string]serviceResourceChange) {
	changes := make([]*route53.Change, 0, len(*updates))
	for _, update := range *updates {
		records := make([]*route53.ResourceRecord, len(update.addresses))
		for idx, ip := range update.addresses {
			records[idx] = &route53.ResourceRecord{
				Value: aws.String(ip),
			}
		}
		changes = append(changes, &route53.Change{
			Action: aws.String("UPSERT"),
			ResourceRecordSet: &route53.ResourceRecordSet{
				Name:            aws.String(update.name),
				Type:            aws.String("A"), // TODO: Check for ipv6!
				ResourceRecords: records,
				TTL:             aws.Int64(15),
			},
		})
	}

	t := time.Now()
	change := &route53.ChangeResourceRecordSetsInput{
		ChangeBatch: &route53.ChangeBatch{
			Changes: changes,
			Comment: aws.String(fmt.Sprintf("Updated by consul53 at %v", t.Format(time.RFC3339))),
		},
		HostedZoneId: aws.String(zone),
	}

	if dryRun {
		fmt.Println(change)
	} else {
		log.Info("Would have updated Amazon, but no code = sad panda")
		//resp, err := serviceClient.ChangeResourceRecordSets(params)
	}
}

func watchService(catalog *consul.Catalog, service string, updates chan serviceResourceChange) {
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
		updates <- serviceResourceChange{name: service, addresses: serviceIPs, timestamp: time.Now()}
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
