import boto3
import unicodedata

# ------------------- 설정 -------------------
# 작업을 수행할 S3 버킷 이름을 입력하세요.
BUCKET_NAME = 'base-inbrain-resource'

# 검사를 원하는 특정 폴더(prefix)를 지정하세요. 버킷 전체를 검사하려면 빈 문자열('')로 두세요.
# 예: 'videos/raw/'
PREFIX_TO_SCAN = 'lectures/'

# True로 설정하면 삭제 대상 파일만 출력하고 실제 삭제는 하지 않습니다.
# 삭제 대상 목록을 충분히 확인한 후, False로 변경하여 실제 삭제를 진행하세요.
DRY_RUN = False
# -------------------------------------------

def find_and_delete_nfd_files():
    """S3 버킷의 특정 prefix에서 NFD 형식의 키를 가진 객체를 찾아 삭제합니다."""

    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')

    found_count = 0
    deleted_count = 0

    scan_target = f"'{BUCKET_NAME}' 버킷"
    if PREFIX_TO_SCAN:
        scan_target += f"의 '{PREFIX_TO_SCAN}' 폴더"

    print(f"== {scan_target}에서 NFD 파일 검사를 시작합니다. ==")
    if DRY_RUN:
        print("== [안내] DRY RUN 모드입니다. 실제 파일 삭제는 이루어지지 않습니다. ==")
    else:
        print("== [경고] 실제 삭제 모드입니다. 스크립트가 실행되면 파일이 영구적으로 삭제됩니다. ==")
    print("-" * 60)

    try:
        # Paginator 호출 시 Prefix 인자 추가
        pages = paginator.paginate(Bucket=BUCKET_NAME, Prefix=PREFIX_TO_SCAN)

        for page in pages:
            if 'Contents' not in page:
                continue

            for obj in page['Contents']:
                key = obj['Key']

                normalized_key = unicodedata.normalize('NFC', key)

                if key != normalized_key:
                    found_count += 1
                    print(f"\n[NFD 파일 발견] '{key}'")

                    if not DRY_RUN:
                        try:
                            s3.delete_object(Bucket=BUCKET_NAME, Key=key)
                            print(f"  -> [삭제 완료] 파일이 성공적으로 삭제되었습니다.")
                            deleted_count += 1
                        except Exception as e:
                            print(f"  -> [삭제 실패] 파일 삭제 중 오류 발생: {e}")
                    else:
                        print(f"  -> [Dry Run] 실제 삭제를 건너뜁니다. (NFC 변환 시: '{normalized_key}')")

    except Exception as e:
        print(f"\n오류: S3 버킷 처리 중 문제가 발생했습니다. ({e})")
        return

    print("\n" + "=" * 60)
    print("모든 작업이 완료되었습니다.")
    print(f"- 총 {found_count}개의 NFD 형식 파일을 찾았습니다.")
    if not DRY_RUN:
        print(f"- 총 {deleted_count}개의 파일을 삭제했습니다.")
    print("=" * 60)


if __name__ == '__main__':
    find_and_delete_nfd_files()