package client

import (
 "context"
 "os"
 "os/exec"
 "path/filepath"
 "testing"
 "time"
)

func TestReviewCompletedFrameCacheDoesNotQueueBehindUnrelatedVideo(t *testing.T) {
 video:=filepath.Join(t.TempDir(),"synthetic.mp4")
 if err:=os.Mkdir(video+".frames",0700);err!=nil {t.Fatal(err)}
 if err:=os.WriteFile(filepath.Join(video+".frames",".complete-v2"),[]byte("complete\n"),0600);err!=nil {t.Fatal(err)}
 if err:=os.WriteFile(filepath.Join(video+".frames","frame-01.jpg"),[]byte("fake"),0600);err!=nil {t.Fatal(err)}
 frameExtraction<-struct{}{}
 defer func(){<-frameExtraction}()
 ctx,cancel:=context.WithTimeout(context.Background(),20*time.Millisecond)
 defer cancel()
 frames,err:=extractFrames(ctx,video,"missing-ffmpeg")
 if err!=nil || len(frames)!=1 {t.Fatalf("completed cache blocked by unrelated work: frames=%v err=%v",frames,err)}
}

func TestReviewVideoShorterThanContainerDuration(t *testing.T) {
 bin,err:=exec.LookPath("ffmpeg");if err!=nil {t.Skip("ffmpeg unavailable")}
 video:=filepath.Join(t.TempDir(),"synthetic.mp4")
 cmd:=exec.Command(bin,"-hide_banner","-loglevel","error","-y","-f","lavfi","-i","color=c=red:s=16x16:r=1:d=1","-f","lavfi","-i","anullsrc=r=8000:cl=mono:d=60","-c:v","mpeg4","-threads","1","-c:a","aac",video)
 if out,err:=cmd.CombinedOutput();err!=nil {t.Fatalf("fixture: %v %s",err,out)}
 frames,err:=extractFrames(context.Background(),video,bin)
 if err!=nil || len(frames)==0 {t.Fatalf("valid video lost frames when audio extends duration: frames=%v err=%v",frames,err)}
}
