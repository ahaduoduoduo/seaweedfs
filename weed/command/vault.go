package command

import (
	"context"
	"fmt"
	"github.com/seaweedfs/seaweedfs/weed/vault"
	"os"
	"os/signal"
	"syscall"
)

var cmdVault = &Command{UsageLine: "vault -config=path.json", Short: "serve the encrypted NAS object API", Long: "Runs one authoritative Vault service with relational packing, automatic cloud upload, direct client reads and encrypted checkpoints."}
var cmdVaultRestore = &Command{UsageLine: "vault.restore -config=path.json -ticket=checkpoint.json", Short: "restore Vault and OpenList receipts from a cloud checkpoint", Long: "Requires an empty target directory, the recovery key and OpenList recovery credentials. Never enumerates the cloud provider."}

func init() {
	config := cmdVault.Flag.String("config", "", "Vault JSON configuration")
	cmdVault.Run = func(cmd *Command, args []string) bool {
		c, e := vault.ReadConfig(*config)
		if e == nil {
			var s *vault.Server
			s, e = vault.Open(c)
			if e == nil {
				defer s.Close()
				ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
				defer cancel()
				e = s.Serve(ctx)
			}
		}
		if e != nil {
			fmt.Fprintln(os.Stderr, e)
			SetCommandExitStatus(1)
		}
		return true
	}
	restoreConfig := cmdVaultRestore.Flag.String("config", "", "Vault JSON configuration with empty dir")
	ticket := cmdVaultRestore.Flag.String("ticket", "", "Signed checkpoint ticket")
	cmdVaultRestore.Run = func(cmd *Command, args []string) bool {
		c, e := vault.ReadConfig(*restoreConfig)
		if e == nil {
			e = vault.Restore(context.Background(), c, *ticket)
		}
		if e != nil {
			fmt.Fprintln(os.Stderr, e)
			SetCommandExitStatus(1)
		} else {
			fmt.Println("Vault catalog and OpenList receipts restored")
		}
		return true
	}
}
