package cmdcommon

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

func BindViper(cmd *cobra.Command, names ...string) {
	for _, name := range names {
		err := viper.BindPFlag(name, cmd.PersistentFlags().Lookup(name))
		if err != nil {
			panic(err)
		}
	}
}
