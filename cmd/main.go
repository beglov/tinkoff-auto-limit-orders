package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/tinkoff/invest-api-go-sdk/investgo"
	pb "github.com/tinkoff/invest-api-go-sdk/proto"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type Trade struct {
	FIGI      string
	Operation pb.OrderDirection
	Price     *pb.Quotation
	Count     int64
}

func main() {
	// загружаем конфигурацию для сдк из .yaml файла
	config, err := investgo.LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("config loading error %v", err.Error())
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	defer cancel()
	// сдк использует для внутреннего логирования investgo.Logger
	// для примера передадим uber.zap
	zapConfig := zap.NewDevelopmentConfig()
	zapConfig.EncoderConfig.EncodeTime = zapcore.TimeEncoderOfLayout(time.DateTime)
	zapConfig.EncoderConfig.TimeKey = "time"
	l, err := zapConfig.Build()
	logger := l.Sugar()
	defer func() {
		err := logger.Sync()
		if err != nil {
			log.Printf(err.Error())
		}
	}()
	if err != nil {
		log.Fatalf("logger creating error %v", err)
	}
	// создаем клиента для investAPI, он позволяет создавать нужные сервисы и уже
	// через них вызывать нужные методы
	client, err := investgo.NewClient(ctx, config, logger)
	if err != nil {
		logger.Fatalf("client creating error %v", err.Error())
	}
	defer func() {
		logger.Infof("closing client connection")
		err := client.Stop()
		if err != nil {
			logger.Errorf("client shutdown error %v", err.Error())
		}
	}()

	// создаем клиента для сервиса ордеров
	OrdersService := client.NewOrdersServiceClient()

	filePath := "orders.txt" // path to your txt file

	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	// Read line by line
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, ";")

		// Skip empty lines or lines with less than 4 fields
		if len(fields) != 4 {
			continue
		}

		var operation pb.OrderDirection

		switch fields[1] {
		case "BUY":
			operation = pb.OrderDirection_ORDER_DIRECTION_BUY
		case "SELL":
			operation = pb.OrderDirection_ORDER_DIRECTION_SELL
		default:
			operation = pb.OrderDirection_ORDER_DIRECTION_UNSPECIFIED
		}

		// Parse the fields
		trade := Trade{
			FIGI:      fields[0],
			Operation: operation,
		}

		// Parse price
		priceParts := strings.Split(fields[2], ".")
		units, err := strconv.Atoi(priceParts[0])
		if err != nil {
			log.Printf("error parsing price: %v", err)
			continue
		}
		nano, err := strconv.Atoi(priceParts[1])
		if err != nil {
			log.Printf("error parsing price: %v", err)
			continue
		}
		trade.Price = &pb.Quotation{
			Units: int64(units),
			Nano:  int32(nano),
		}

		// Parse count
		_, err = fmt.Sscanf(fields[3], "%d", &trade.Count)
		if err != nil {
			log.Printf("error parsing count: %v", err)
			continue
		}

		fmt.Printf("FIGI: %s, Operation: %s, Price: %v, Count: %d\n", trade.FIGI, trade.Operation, trade.Price, trade.Count)

		postResp, err := OrdersService.PostOrder(&investgo.PostOrderRequest{
			InstrumentId: trade.FIGI,
			Quantity:     trade.Count,
			Price:        trade.Price,
			Direction:    trade.Operation,
			AccountId:    config.AccountId,
			OrderType:    pb.OrderType_ORDER_TYPE_LIMIT,
			OrderId:      investgo.CreateUid(),
		})
		if err != nil {
			logger.Errorf("post order %v\n", err.Error())
		} else {
			fmt.Printf("post order resp = %v\n", postResp.GetExecutionReportStatus().String())
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("failed to read file: %v", err)
	}
}
