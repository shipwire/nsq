package nsqd

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/bitly/nsq/nsqd"
	"github.com/bitly/nsq/util"
	"github.com/hashicorp/serf/command/agent"
	"github.com/hashicorp/serf/serf"
	"github.com/mreiferson/go-options"
	"github.com/shipwire/ansqd/internal/polity"
)

func main() {
	flagSet.Parse(os.Args[1:])

	rand.Seed(time.Now().UTC().UnixNano())

	if *showVersion {
		fmt.Println(util.Version("nsqd"))
		return
	}

	var cfg map[string]interface{}
	if *config != "" {
		_, err := toml.DecodeFile(*config, &cfg)
		if err != nil {
			log.Fatalf("ERROR: failed to load config file %s - %s", *config, err.Error())
		}
	}
	if v, exists := cfg["tls_required"]; exists {
		var tlsRequired tlsRequiredOption
		err := tlsRequired.Set(fmt.Sprintf("%v", v))
		if err == nil {
			cfg["tls_required"] = tlsRequired.String()
		}
	}

	ag, err := agent.Create(agent.DefaultConfig(), serf.DefaultConfig(), os.Stdout)
	if err != nil {
		log.Fatal(err)
	}

	p := polity.CreateWithAgent(ag)
	a = auditor{
		p,
		ag,
		make(map[string]*Host),
		&sync.Mutex{},
	}

	nsqd.Delegate = &delegate{}

	opts := nsqd.NewNSQDOptions()
	options.Resolve(opts, flagSet, cfg)
	n = nsqd.NewNSQD(opts)

	n.LoadMetadata()
	err = n.PersistMetadata()
	if err != nil {
		log.Fatalf("ERROR: failed to persist metadata - %s", err.Error())
	}

	n.Main()
	<-signalChan
	n.Exit()
}
