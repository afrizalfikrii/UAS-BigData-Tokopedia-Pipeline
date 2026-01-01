import functions_framework
import json
import os
from google_play_scraper import Sort, reviews
from google.cloud import pubsub_v1
from datetime import datetime

publisher = pubsub_v1.PublisherClient()

PROJECT_ID = "ipsd-pak-jumadi"
TOPIC_ID = "SCRP_Unit_1"

topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

@functions_framework.http
def scrape_to_pubsub(request):
    """
    HTTP Cloud Function: Menerima request, scrape data, kirim ke Pub/Sub.
    Input JSON: {"app_id": "com.tokopedia.tkpd"}
    """
    request_json = request.get_json(silent=True)
    
    # Default fallback
    app_id = "com.tokopedia.tkpd" 
    
    if request_json and 'app_id' in request_json:
        app_id = request_json['app_id']

    print(f"🔄 Memulai Realtime Scraping untuk: {app_id}")

    try:
        # 2. PROSES SCRAPING
        result, _ = reviews(
            app_id,
            lang='id',
            country='id',
            sort=Sort.NEWEST,
            count=50 
        )

        success_count = 0

        # 3. KIRIM KE PUB/SUB
        for r in result:
            payload = {
                "review_id": r['reviewId'],
                "app_id": app_id,
                "user_name": r['userName'],
                "content": r['content'],
                "score": r['score'],
                "at": r['at'].isoformat(),            # PERUBAHAN 1: Pakai isoformat() biar rapi
                "scraped_at": datetime.now().isoformat()
            }

            # Convert ke JSON String -> Bytes
            data_str = json.dumps(payload, ensure_ascii=False)
            data_bytes = data_str.encode("utf-8")

            # PERUBAHAN 2 (CRUCIAL): Tambahkan .result()
            # Ini memaksa fungsi menunggu sampai data benar-benar masuk Pub/Sub
            future = publisher.publish(topic_path, data_bytes)
            future.result() 
            
            success_count += 1

        print(f"✅ Sukses mengirim {success_count} data ke Pub/Sub.")
        
        return json.dumps({
            "status": "success",
            "app_id": app_id,
            "sent_count": success_count
        }), 200

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return json.dumps({"error": str(e)}), 500