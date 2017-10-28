package cmdcommon

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

func BindViper(flags *pflag.FlagSet, names ...string) {
	for _, name := range names {
		err := viper.BindPFlag(name, flags.Lookup(name))
		if err != nil {
			panic(err)
		}
	}
}
