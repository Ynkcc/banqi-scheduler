package scheduler

import (
	"context"
	"fmt"
	"log"
	"strings"

	"banqi/server/internal/r2"
	"banqi/server/internal/store"
	pb "banqi/server/pb"
)

// ReportEpisode 登记 episode 元数据并签发 R2 预签名 PUT（数据由 worker 直传）。
func (s *Server) ReportEpisode(ctx context.Context, req *pb.EpisodeMeta) (*pb.EpisodeAck, error) {
	task, ok := s.lookupTask(req.TaskId)
	if !ok {
		return &pb.EpisodeAck{Accepted: false, Message: "unknown_task_id:" + req.TaskId}, nil
	}
	if task.WorkerID != req.WorkerId {
		return &pb.EpisodeAck{Accepted: false, Message: fmt.Sprintf("worker_mismatch task_owner=%s got=%s", task.WorkerID, req.WorkerId)}, nil
	}
	if task.NetworkSha != req.NetworkSha {
		return &pb.EpisodeAck{Accepted: false, Message: fmt.Sprintf("network_sha_mismatch task=%s got=%s", task.NetworkSha, req.NetworkSha)}, nil
	}
	objID, err := newID()
	if err != nil {
		return nil, err
	}
	key := r2.EpisodeKey(req.NetworkSha, objID)
	url, err := s.r2.PresignPut(ctx, key, req.ContentLength)
	if err != nil {
		return nil, fmt.Errorf("presign episode put %s: %w", key, err)
	}
	if err := s.store.InsertEpisode(ctx, store.Episode{
		WorkerID: req.WorkerId, TaskID: req.TaskId, NetworkSha: req.NetworkSha,
		GameCount: int(req.GameCount), TotalSteps: int(req.TotalSteps), Winner: int(req.Winner),
		ObjectKey: key, Kind: int(req.Kind),
	}); err != nil {
		return nil, fmt.Errorf("insert episode worker=%s task=%s: %w", req.WorkerId, req.TaskId, err)
	}
	log.Printf("[episode] worker=%s task=%s network=%s games=%d steps=%d kind=%s -> %s",
		req.WorkerId, req.TaskId, req.NetworkSha, req.GameCount, req.TotalSteps, DataKindName(req.Kind), key)
	return &pb.EpisodeAck{Accepted: true, UploadUrl: url, ObjectKey: key}, nil
}

// SignNetworkUpload 为 trainer 签发网络直传 R2 的预签名 PUT。
// R2 凭据只在调度器持有：对象键由 sha + 权重格式决定
// （networks/<sha>.<onnx|pt|nnue>），trainer 零存储配置。
func (s *Server) SignNetworkUpload(ctx context.Context, req *pb.SignNetworkUploadRequest) (*pb.SignNetworkUploadAck, error) {
	sha := strings.TrimSpace(req.Sha)
	if len(sha) != 64 {
		return &pb.SignNetworkUploadAck{Accepted: false,
			Message: fmt.Sprintf("invalid_sha_len=%d (want 64 hex chars)", len(sha))}, nil
	}
	format, err := normalizeFormat(req.Format)
	if err != nil {
		return &pb.SignNetworkUploadAck{Accepted: false, Message: err.Error()}, nil
	}
	key := r2.NetworkKey(sha, format)
	url, err := s.r2.PresignPut(ctx, key, req.ContentLength)
	if err != nil {
		return nil, fmt.Errorf("presign network upload %s: %w", key, err)
	}
	log.Printf("[network] sign upload trainer=%s key=%s len=%d", req.TrainerId, key, req.ContentLength)
	return &pb.SignNetworkUploadAck{Accepted: true, UploadUrl: url, ObjectKey: key}, nil
}

// ListEpisodes 游标分页下发已登记 episode 的预签名 GET 列表（trainer 消费端）。
// req.Kind 指定时只返回该数据类别的对象：消费方据此避免下载无法消费的对象。
func (s *Server) ListEpisodes(ctx context.Context, req *pb.ListEpisodesRequest) (*pb.ListEpisodesReply, error) {
	kind := store.KindAny
	if req.Kind != nil {
		kind = int(req.GetKind())
	}
	keys, err := s.store.ListEpisodeKeys(ctx, req.AfterKey, int(req.Limit), kind)
	if err != nil {
		return nil, fmt.Errorf("list episode keys after=%q kind=%d: %w", req.AfterKey, kind, err)
	}
	reply := &pb.ListEpisodesReply{}
	for _, k := range keys {
		url, err := s.r2.PresignGet(ctx, k)
		if err != nil {
			return nil, fmt.Errorf("presign episode %s: %w", k, err)
		}
		reply.Objects = append(reply.Objects, &pb.EpisodeObject{ObjectKey: k, DownloadUrl: url})
	}
	reply.HasMore = len(keys) == int(req.Limit) && req.Limit > 0
	return reply, nil
}
