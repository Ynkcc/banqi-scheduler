package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"banqi/server/internal/api"
	"banqi/server/internal/r2"
	"banqi/server/internal/scheduler"
	"banqi/server/internal/store"
	pb "banqi/server/pb"

	"google.golang.org/grpc"
)

type config struct {
	listen                  string
	variant                 string
	sqlitePath              string
	r2Bucket                string
	gamesPerTask            int
	gatekeeperGames         int
	threadsBaseline         int
	initialRevealed         int
	dataKind                pb.DataKind
	elo0, elo1, alpha, beta float64
	minClientVersion        string
	httpAddr                string
	workerOnlineSeconds     int
}

func loadConfig() config {
	c := config{
		listen:              envOr("SCHEDULER_LISTEN", ":50052"),
		variant:             envOr("SCHEDULER_VARIANT", "4x8"),
		sqlitePath:          envOr("SCHEDULER_DB", "scheduler.db"),
		r2Bucket:            envOr("SCHEDULER_R2_BUCKET", "banqi"),
		gamesPerTask:        envInt("SCHEDULER_GAMES_PER_TASK", 16),
		gatekeeperGames:     envInt("SCHEDULER_GATEKEEPER_PAIRS", 400),
		threadsBaseline:     envInt("SCHEDULER_THREADS_BASELINE", 0),
		initialRevealed:     envInt("SCHEDULER_INITIAL_REVEALED", 0),
		elo0:                envFloat("SCHEDULER_SPRT_ELO0", 0),
		elo1:                envFloat("SCHEDULER_SPRT_ELO1", 30),
		alpha:               envFloat("SCHEDULER_SPRT_ALPHA", 0.05),
		beta:                envFloat("SCHEDULER_SPRT_BETA", 0.05),
		minClientVersion:    os.Getenv("SCHEDULER_MIN_CLIENT_VERSION"),
		httpAddr:            envOr("SCHEDULER_HTTP_ADDR", "127.0.0.1:8080"),
		workerOnlineSeconds: envInt("SCHEDULER_WORKER_ONLINE_SECONDS", 60),
	}
	kind, err := scheduler.ParseDataKind(envOr("SCHEDULER_DATA_KIND", "resnet"))
	if err != nil {
		log.Printf("[config] %v，按 resnet 处理", err)
	}
	c.dataKind = kind
	return c
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		log.Printf("[config] invalid int %s=%q, using %d", key, v, def)
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
		log.Printf("[config] invalid float %s=%q, using %g", key, v, def)
	}
	return def
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("scheduler: %v", err)
	}
}

// shutdownGrace：优雅关闭时等待在飞请求与日志刷盘的上限。
const shutdownGrace = 5 * time.Second

// run 编排调度器生命周期：启动失败即返回错误（defer 生效后才退出），
// 收到 SIGINT/SIGTERM 后按 gRPC → WebUI 顺序优雅关闭。
func run() error {
	showHelp := flag.Bool("h", false, "show environment variable help")
	flag.Parse()
	if *showHelp {
		log.Println("env: SCHEDULER_LISTEN, SCHEDULER_VARIANT, SCHEDULER_DB, SCHEDULER_R2_BUCKET,",
			"SCHEDULER_GAMES_PER_TASK, SCHEDULER_GATEKEEPER_PAIRS, SCHEDULER_THREADS_BASELINE,",
			"SCHEDULER_INITIAL_REVEALED, SCHEDULER_DATA_KIND (resnet|nnue),",
			"SCHEDULER_SPRT_ELO0, SCHEDULER_SPRT_ELO1, SCHEDULER_SPRT_ALPHA, SCHEDULER_SPRT_BETA,",
			"SCHEDULER_MIN_CLIENT_VERSION, SCHEDULER_HTTP_ADDR, SCHEDULER_WORKER_ONLINE_SECONDS,",
			"AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_URL_S3, AWS_S3_PATH_STYLE")
		return nil
	}
	cfg := loadConfig()

	st, err := store.Open(cfg.sqlitePath)
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	defer st.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	presigner, err := r2.New(ctx, cfg.r2Bucket)
	if err != nil {
		return fmt.Errorf("r2 presigner: %w", err)
	}

	srv, err := scheduler.New(ctx, scheduler.Config{
		Variant:          cfg.variant,
		GamesPerTask:     cfg.gamesPerTask,
		GatekeeperGames:  cfg.gatekeeperGames,
		ThreadsBaseline:  cfg.threadsBaseline,
		InitialRevealed:  cfg.initialRevealed,
		DataKind:         cfg.dataKind,
		SprtElo0:         cfg.elo0,
		SprtElo1:         cfg.elo1,
		SprtAlpha:        cfg.alpha,
		SprtBeta:         cfg.beta,
		MinClientVersion: cfg.minClientVersion,
	}, st, presigner)
	if err != nil {
		return fmt.Errorf("scheduler: %w", err)
	}

	// WebUI 与 gRPC 同进程：监听失败只记日志，不影响 gRPC 调度。
	webui := api.New(st, srv, time.Duration(cfg.workerOnlineSeconds)*time.Second)
	webuiSrv := webui.HTTPServer(cfg.httpAddr)
	go func() {
		log.Printf("[webui] listening on %s variant=%s paused=%v initial_revealed=%d data_kind=%s",
			cfg.httpAddr, cfg.variant, srv.Runtime().Paused, srv.Runtime().InitialRevealed,
			scheduler.DataKindName(srv.Runtime().DataKind))
		if err := webuiSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("[webui] serve stopped: %v（gRPC 调度不受影响）", err)
		}
	}()

	lis, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.listen, err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterSchedulerServiceServer(grpcServer, srv)
	log.Printf("[scheduler] listening on %s db=%s bucket=%s sprt(elo0=%g,elo1=%g,alpha=%g,beta=%g)",
		cfg.listen, cfg.sqlitePath, cfg.r2Bucket, cfg.elo0, cfg.elo1, cfg.alpha, cfg.beta)

	serveErr := make(chan error, 1)
	go func() { serveErr <- grpcServer.Serve(lis) }()

	select {
	case err := <-serveErr:
		return fmt.Errorf("grpc serve: %w", err)
	case <-ctx.Done():
		log.Printf("[scheduler] 收到退出信号，开始优雅关闭")
	}

	grpcServer.GracefulStop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	if err := webuiSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("[webui] shutdown: %v", err)
	}
	log.Printf("[scheduler] 已退出")
	return nil
}
