package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"strconv"

	"banqi/server/internal/r2"
	"banqi/server/internal/scheduler"
	"banqi/server/internal/store"
	pb "banqi/server/pb"

	"google.golang.org/grpc"
)

type config struct {
	listen      string
	variant     string
	sqlitePath  string
	r2Bucket    string
	gamesPerTask int
	gatekeeperGames int
	elo0, elo1, alpha, beta float64
	minClientVersion string
}

func loadConfig() config {
	c := config{
		listen:          envOr("SCHEDULER_LISTEN", ":50052"),
		variant:         envOr("SCHEDULER_VARIANT", "4x8"),
		sqlitePath:      envOr("SCHEDULER_DB", "scheduler.db"),
		r2Bucket:        envOr("SCHEDULER_R2_BUCKET", "banqi"),
		gamesPerTask:    envInt("SCHEDULER_GAMES_PER_TASK", 16),
		gatekeeperGames: envInt("SCHEDULER_GATEKEEPER_PAIRS", 400),
		elo0:            envFloat("SCHEDULER_SPRT_ELO0", 0),
		elo1:            envFloat("SCHEDULER_SPRT_ELO1", 30),
		alpha:           envFloat("SCHEDULER_SPRT_ALPHA", 0.05),
		beta:            envFloat("SCHEDULER_SPRT_BETA", 0.05),
		minClientVersion: os.Getenv("SCHEDULER_MIN_CLIENT_VERSION"),
	}
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
	showHelp := flag.Bool("h", false, "show environment variable help")
	flag.Parse()
	if *showHelp {
		log.Println("env: SCHEDULER_LISTEN, SCHEDULER_VARIANT, SCHEDULER_DB, SCHEDULER_R2_BUCKET,",
			"SCHEDULER_GAMES_PER_TASK, SCHEDULER_GATEKEEPER_PAIRS,",
			"SCHEDULER_SPRT_ELO0, SCHEDULER_SPRT_ELO1, SCHEDULER_SPRT_ALPHA, SCHEDULER_SPRT_BETA,",
			"SCHEDULER_MIN_CLIENT_VERSION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_URL_S3")
		return
	}
	cfg := loadConfig()

	st, err := store.Open(cfg.sqlitePath)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	defer st.Close()

	ctx := context.Background()
	presigner, err := r2.New(ctx, cfg.r2Bucket)
	if err != nil {
		log.Fatalf("r2 presigner: %v", err)
	}

	srv := scheduler.New(scheduler.Config{
		Variant:          cfg.variant,
		GamesPerTask:     cfg.gamesPerTask,
		GatekeeperGames:  cfg.gatekeeperGames,
		SprtElo0:         cfg.elo0,
		SprtElo1:         cfg.elo1,
		SprtAlpha:        cfg.alpha,
		SprtBeta:         cfg.beta,
		MinClientVersion: cfg.minClientVersion,
	}, st, presigner)

	lis, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		log.Fatalf("listen %s: %v", cfg.listen, err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterSchedulerServiceServer(grpcServer, srv)
	log.Printf("[scheduler] listening on %s db=%s bucket=%s sprt(elo0=%g,elo1=%g,alpha=%g,beta=%g)",
		cfg.listen, cfg.sqlitePath, cfg.r2Bucket, cfg.elo0, cfg.elo1, cfg.alpha, cfg.beta)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
