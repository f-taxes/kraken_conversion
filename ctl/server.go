package ctl

import (
	"context"
	"net"

	"github.com/f-taxes/kraken_conversion/converter"
	"github.com/f-taxes/kraken_conversion/global"
	pb "github.com/f-taxes/kraken_conversion/proto"
	"github.com/kataras/golog"
	"google.golang.org/grpc"
)

type PluginCtl struct {
	pb.UnimplementedPluginCtlServer
}

func (s *PluginCtl) ConvertPricesInTrade(ctx context.Context, job *pb.TradeConversionJob) (*pb.Trade, error) {
	if job.Trade.FeeCurrency == job.TargetCurrency {
		job.Trade.FeeC = job.Trade.Fee
		job.Trade.FeePriceC = "1"
		job.Trade.FeeConvertedBy = global.Plugin.ID
	}

	if job.Trade.Quote == job.TargetCurrency {
		job.Trade.PriceC = job.Trade.Price
		job.Trade.PriceConvertedBy = global.Plugin.ID
		return job.Trade, nil
	}

	price, err := converter.PriceAtTime(job.Trade.Asset, job.TargetCurrency, job.Trade.Ts.AsTime())
	if err != nil {
		return nil, err
	}

	job.Trade.PriceC = price.String()
	job.Trade.ValueC = price.Mul(global.StrToDecimal(job.Trade.Amount)).String()

	if !price.IsZero() {
		job.Trade.PriceConvertedBy = global.Plugin.ID
	}

	feePrice, err := converter.PriceAtTime(job.Trade.FeeCurrency, job.TargetCurrency, job.Trade.Ts.AsTime())
	if err != nil {
		return nil, err
	}

	if price.IsZero() {
		return job.Trade, nil
	}

	job.Trade.FeePriceC = feePrice.String()
	job.Trade.FeeC = global.StrToDecimal(job.Trade.Fee).Mul(feePrice).String()

	if !feePrice.IsZero() {
		job.Trade.FeeConvertedBy = global.Plugin.ID
	}

	return job.Trade, nil
}

func (s *PluginCtl) ConvertPricesInTransfer(ctx context.Context, job *pb.TransferConversionJob) (*pb.Transfer, error) {
	if job.Transfer.FeeCurrency == job.TargetCurrency {
		job.Transfer.FeeC = job.Transfer.Fee
		job.Transfer.FeeConvertedBy = global.Plugin.ID
		return job.Transfer, nil
	}

	price, err := converter.PriceAtTime(job.Transfer.FeeCurrency, job.TargetCurrency, job.Transfer.Ts.AsTime())
	if err != nil {
		return nil, err
	}

	if price.IsZero() {
		return job.Transfer, nil
	}

	job.Transfer.FeePriceC = price.String()
	job.Transfer.FeeC = global.StrToDecimal(job.Transfer.Fee).Mul(price).String()
	job.Transfer.FeeConvertedBy = global.Plugin.ID
	return job.Transfer, nil
}

func Start(address string) {
	srv := &PluginCtl{}
	lis, err := net.Listen("tcp", address)
	if err != nil {
		golog.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterPluginCtlServer(s, srv)
	golog.Infof("Ctl server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		golog.Fatalf("failed to serve: %v", err)
	}
}
