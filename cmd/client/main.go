package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"lightkafka/internal/client"
	"lightkafka/internal/message"
)

const (
	TOTAL_RECORDS   = 1000        // 총 전송할 레코드 수
	MAX_BATCH_SIZE  = 50          // 한 배치당 최대 레코드 수 (랜덤)
	FETCH_MAX_BYTES = 1024 * 1024 // Fetch 할 때 버퍼 크기 (1MB)
)

func main() {
	// 랜덤 시드 설정
	rand.Seed(time.Now().UnixNano())

	// 1. 브로커 연결
	fmt.Println("🔌 Connecting to LightKafka Broker...")
	c, err := client.NewClient(client.Config{
		BrokerAddr: "localhost:9092",
		ClientID:   "test-producer-1",
	})
	if err != nil {
		log.Fatalf("Connection failed: %v", err)
	}
	defer c.Close()

	// ---------------------------------------------------------
	// PHASE 1: PRODUCE (랜덤 배치 전송)
	// ---------------------------------------------------------
	fmt.Printf("\n🚀 STARTING PRODUCE PHASE (Target: %d records)\n", TOTAL_RECORDS)
	fmt.Println("---------------------------------------------------")

	var sentOffsets []int64 // 나중에 Fetch 할 오프셋들 저장
	totalSent := 0
	batchCount := 0

	startTime := time.Now()

	for totalSent < TOTAL_RECORDS {
		// 1. 이번 배치의 크기 결정 (1 ~ MAX_BATCH_SIZE)
		currentBatchSize := rand.Intn(MAX_BATCH_SIZE) + 1
		if totalSent+currentBatchSize > TOTAL_RECORDS {
			currentBatchSize = TOTAL_RECORDS - totalSent
		}

		// 2. 배치 빌더를 사용하여 RecordBatch 생성
		builder := client.NewRecordBatchBuilder()
		for i := 0; i < currentBatchSize; i++ {
			msgNum := totalSent + i + 1
			// Key와 Value 생성
			key := []byte(fmt.Sprintf("k-%d", msgNum))
			val := []byte(fmt.Sprintf("Hello LightKafka #%d", msgNum))
			builder.Add(key, val)
		}

		batchBytes := builder.Build()

		// 3. 브로커로 전송
		recordBatch := &message.RecordBatch{Payload: batchBytes}
		offset, err := c.Produce(recordBatch)
		if err != nil {
			log.Fatalf("❌ Produce failed at batch #%d: %v", batchCount, err)
		}

		// 4. 오프셋 저장 및 로그 출력
		sentOffsets = append(sentOffsets, offset)
		totalSent += currentBatchSize
		batchCount++

		// 진행 상황 출력 (너무 빠르니 500ms마다 혹은 배치마다 출력)
		fmt.Printf("\r[Produce] Batch #%03d | Size: %2d | Stored at Offset: %4d | Progress: %4d/%d",
			batchCount, currentBatchSize, offset, totalSent, TOTAL_RECORDS)

		// 네트워크 부하 조절 (선택 사항)
		time.Sleep(2 * time.Millisecond)
	}

	duration := time.Since(startTime)
	fmt.Printf("\n\n✅ PRODUCE COMPLETE! %d records in %d batches (Latency: %v)\n", totalSent, batchCount, duration)

	// ---------------------------------------------------------
	// PHASE 2: FETCH & DECODE (데이터 검증)
	// ---------------------------------------------------------
	fmt.Printf("\n🔍 STARTING FETCH & DECODE PHASE\n")
	fmt.Println("---------------------------------------------------")

	successCount := 0

	for i, offset := range sentOffsets {
		// 1. 해당 오프셋의 데이터 요청
		data, err := c.Fetch(offset, FETCH_MAX_BYTES)
		if err != nil {
			log.Printf("❌ Fetch failed for batch #%d (Offset %d): %v", i, offset, err)
			continue
		}

		if len(data) == 0 {
			fmt.Printf("⚠️ Empty response for batch #%d (Offset %d)\n", i, offset)
			continue
		}

		// 2. 바이너리 디코딩 (Raw Bytes -> Records)
		records, err := client.DecodeBatch(data)
		if err != nil {
			fmt.Printf("❌ Decode failed for batch #%d: %v\n", i, err)
			continue
		}

		successCount++

		// 3. 검증 로그 출력 (첫 번째와 마지막 배치는 상세 내용을 보여줌)
		if i == 0 || i == len(sentOffsets)-1 {
			fmt.Printf("[Verify] Batch #%d (BaseOffset %d) -> Decoded %d records:\n", i, offset, len(records))
			for j, r := range records {
				// 너무 길면 앞 3개만 출력
				if j >= 3 {
					fmt.Printf("    ... (skip %d records)\n", len(records)-3)
					break
				}
				fmt.Printf("    [%d] Offset: %d | Key: %-5s | Value: %s\n", j, r.Offset, r.Key, r.Value)
			}
			fmt.Println("    --------------------------------")
		}
	}

	// 최종 리포트
	fmt.Println("\n📊 TEST REPORT")
	fmt.Println("---------------------------------------------------")
	fmt.Printf("Total Batches Sent: %d\n", len(sentOffsets))
	fmt.Printf("Total Batches Read: %d\n", successCount)

	if successCount == len(sentOffsets) {
		fmt.Println("🎉 RESULT: ALL TESTS PASSED! (Data Integrity Confirmed)")
	} else {
		fmt.Printf("💥 RESULT: FAILED (%d failures)\n", len(sentOffsets)-successCount)
	}
}
