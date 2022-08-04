package cmd

import (
	"github.com/HarrisChu/nebula-opencypher-adapter/pkg"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var port int

var RootCmd = &cobra.Command{
	Use:   "adapter",
	Short: "A opencypher adapter to nebula graph",

	CompletionOptions: cobra.CompletionOptions{HiddenDefaultCmd: false},
	RunE: func(cmd *cobra.Command, args []string) error {
		server := pkg.NewServer()
		server.RunForever(port)

		return nil
	},
}

func init() {
	flags := pflag.NewFlagSet("", pflag.ContinueOnError)
	flags.StringVarP(&pkg.Opts.Address, "address", "a", "", "nebula address")
	flags.StringVarP(&pkg.Opts.User, "user", "u", "root", "nebula user")
	flags.StringVarP(&pkg.Opts.Password, "password", "p", "nebula", "nebula password")
	flags.StringVarP(&pkg.Opts.Space, "space", "s", "", "nebula space")
	flags.IntVar(&port, "port", 8000, "nebula space")
	cobra.MarkFlagRequired(flags, "address")
	cobra.MarkFlagRequired(flags, "space")
	RootCmd.PersistentFlags().AddFlagSet(flags)
}
