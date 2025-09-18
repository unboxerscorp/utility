import boto3
import ffmpeg
import os
import sys
import re

# --- 사용자 설정 ---
LOCAL_TEST_FILE = 'sample.mov'
FFMPEG_PATH = '/opt/homebrew/bin/ffmpeg'
BUCKET_NAME = 'base-inbrain-resource'
PREFIX_TO_SCAN = 'lectures/'
CLOUDFRON_DOMAIN = 'media.basemath.co.kr'
BITRATE_THRESHOLD = 5000000
TARGET_BITRATE = 4000000
DRY_RUN = False
folders_to_process = [
    "공수2 1강",
    "공수2 2강",
    "공수2 3강",
    "공수2 4강",
    "공수2 5강",
    "공수2 6강",
]
# -------------------------------------------

def run_ffmpeg_with_progress(ffmpeg_process):
    """FFmpeg 프로세스를 실행하고 실시간 진행률을 출력합니다."""
    process = ffmpeg_process.run_async(cmd=FFMPEG_PATH, pipe_stderr=True)

    while True:
        line = process.stderr.readline()
        if not line:
            break

        line = line.decode('utf-8').strip()
        # "time=00:00:15.32" 와 "speed=2.5x" 같은 정보를 찾습니다.
        time_match = re.search(r'time=(\S+)', line)
        speed_match = re.search(r'speed=(\S+)', line)

        if time_match and speed_match:
            progress_time = time_match.group(1)
            speed = speed_match.group(1)
            # 한 줄에 계속 갱신해서 보여주기
            print(f"    ⏳ 진행 시간: {progress_time} / 속도: {speed}", end='\r')

    # 최종 결과 확인
    return_code = process.wait()
    print() # 줄바꿈
    if return_code != 0:
        # wait() 이후에 에러 내용을 다시 읽습니다.
        error_output = process.stderr.read().decode('utf8')
        raise ffmpeg.Error('ffmpeg', None, error_output)


def test_local_transcoding():
    """로컬 파일로 FFmpeg 변환을 테스트하고 진행률을 보여줍니다."""
    print("="*70)
    print(f"1. 로컬 FFmpeg 사전 테스트를 시작합니다 (대상: '{LOCAL_TEST_FILE}')...")
    output_file = f"test_output_{os.path.basename(LOCAL_TEST_FILE)}"

    if not os.path.exists(LOCAL_TEST_FILE):
        print(f"   [실패] 테스트 파일 '{LOCAL_TEST_FILE}'을 찾을 수 없습니다.")
        return False

    try:
        process = (
            ffmpeg
            .input(LOCAL_TEST_FILE)
            .output(output_file, video_bitrate=TARGET_BITRATE, acodec='copy')
            .global_args('-progress', 'pipe:2') # 진행률 출력을 stderr로 보냄
            .overwrite_output()
        )
        run_ffmpeg_with_progress(process)
        print(f"   [성공] 로컬 테스트 통과. FFmpeg가 정상적으로 작동합니다.")
        return True
    except ffmpeg.Error as e:
        print("\n   [실패] 로컬 테스트 중 FFmpeg 오류가 발생했습니다.", file=sys.stderr)
        print("   --- FFmpeg 상세 오류 ---", file=sys.stderr)
        print(e.stderr, file=sys.stderr)
        return False
    finally:
        if os.path.exists(output_file):
            os.remove(output_file)

def process_videos_in_folder(folder_name):
    """S3 폴더의 동영상을 변환하며 진행률을 보여줍니다."""
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    full_folder_path = f"{PREFIX_TO_SCAN}{folder_name}/"

    print("\n" + "="*70)
    print(f"== '{full_folder_path}' 폴더의 작업을 시작합니다. ==")

    try:
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=full_folder_path)
        # ... (이하 파일 목록 조회 및 비트레이트 확인 로직은 동일)
        for page in pages:
            if 'Contents' not in page: continue
            for obj in page['Contents']:
                # ...
                if not obj['Key'].lower().endswith(('.mov', '.mp4')): continue
                key = obj['Key']
                print("-" * 50)
                print(f"[대상 파일] s3://{BUCKET_NAME}/{key}")

                # ... (비트레이트 확인 로직)
                current_bitrate = 0 # 임시
                try:
                    cloudfront_url = f"https://{CLOUDFRON_DOMAIN}/{key}"
                    probe = ffmpeg.probe(cloudfront_url)
                    video_stream = next((s for s in probe['streams'] if s['codec_type'] == 'video'), None)
                    if not video_stream or 'bit_rate' not in video_stream: continue
                    current_bitrate = int(video_stream['bit_rate'])
                    print(f" -> 비트레이트: {current_bitrate / 1000000:.2f} Mbps")
                except Exception: continue

                if current_bitrate > BITRATE_THRESHOLD:
                    print(f" -> 기준치 초과. 변환을 시작합니다.")
                    if DRY_RUN:
                        print(" -> [Dry Run] 실제 변환/업로드는 건너뜁니다.")
                        continue

                    download_path = f"/tmp/{os.path.basename(key)}"
                    transcoded_path = f"/tmp/transcoded_{os.path.basename(key)}"

                    try:
                        print(" -> S3에서 다운로드 중...")
                        s3_client.download_file(BUCKET_NAME, key, download_path)
                        print(" -> FFmpeg으로 트랜스코딩 중...")

                        process = (
                            ffmpeg
                            .input(download_path)
                            .output(transcoded_path, video_bitrate=TARGET_BITRATE, acodec='copy')
                            .global_args('-progress', 'pipe:2')
                            .overwrite_output()
                        )
                        run_ffmpeg_with_progress(process)

                        print(" -> S3로 업로드 중...")
                        s3_client.upload_file(transcoded_path, BUCKET_NAME, key)
                        print(" -> [성공] 작업이 완료되었습니다.")
                    except ffmpeg.Error as e:
                        print(f"\n -> [오류] FFmpeg 변환 실패:", file=sys.stderr)
                        print(e.stderr, file=sys.stderr)
                    except Exception as e:
                        print(f"\n -> [오류] 파일 처리 중 실패: {e}", file=sys.stderr)
                    finally:
                        if os.path.exists(download_path): os.remove(download_path)
                        if os.path.exists(transcoded_path): os.remove(transcoded_path)
                else:
                    print(" -> 기준치 이하. 작업을 건너뜁니다.")
    except Exception as e:
        print(f"\n[치명적 오류] '{full_folder_path}' 처리 중 문제가 발생했습니다: {e}", file=sys.stderr)

if __name__ == '__main__':
    if not DRY_RUN or test_local_transcoding():
        # ... (이하 실행 로직은 동일)
        print("\n" + "="*70)
        print("2. S3 동영상 변환 작업을 시작합니다.")
        if DRY_RUN: print("== [안내] S3 작업은 DRY RUN 모드입니다. ==")
        else: print("== [경고] S3 작업은 실제 실행 모드입니다. ==")

        print(f"총 {len(folders_to_process)}개의 폴더를 대상으로 작업을 시작합니다.")
        for folder in folders_to_process:
            process_videos_in_folder(folder)

        print("\n" + "="*70)
        print("모든 지정된 작업이 종료되었습니다.")