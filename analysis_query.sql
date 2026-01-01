SELECT
  userName,
  TIMESTAMP(`at`) as waktu_review,  
  content as ulasan,
  score as rating,
  initial_sentiment, 
  
  CASE 
    WHEN LOWER(content) LIKE '%lemot%' OR LOWER(content) LIKE '%berat%' OR LOWER(content) LIKE '%blank%' THEN 'Performa Aplikasi'
    WHEN LOWER(content) LIKE '%saldo%' OR LOWER(content) LIKE '%dana%' OR LOWER(content) LIKE '%kembali%' THEN 'Pembayaran & Refund'
    WHEN LOWER(content) LIKE '%kurir%' OR LOWER(content) LIKE '%kirim%' OR LOWER(content) LIKE '%paket%' THEN 'Pengiriman'
    WHEN LOWER(content) LIKE '%penipu%' OR LOWER(content) LIKE '%bohong%' THEN 'Keamanan'
    ELSE 'Umum/Lainnya'
  END as prediksi_aspek

FROM
  `ipsd-pak-jumadi.tokopedia_data.reviews_raw` 
WHERE score = 1
ORDER BY `at` DESC  
LIMIT 10;