import pandas as pd
import great_expectations as gx
import sys
import requests
import os

def send_slack_notification(message):
    # GitHub Secrets'tan gelen URL'i okur
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if webhook_url:
        payload = {"text": message}
        try:
            requests.post(webhook_url, json=payload)
        except Exception as e:
            print(f"Slack gönderim hatası: {e}")

def run_validation():
    # 1. Veriyi Yükle
    df = pd.read_csv("data/amazon_orders.csv", low_memory=False)
    
    # 2. GE Ayarları
    context = gx.get_context()
    suite = context.suites.add(gx.ExpectationSuite(name="ci_suite"))
    
    # Beklentiler
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="Order ID"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="Qty", min_value=0))

    # 3. Validation Tanımı
    data_source = context.data_sources.add_pandas(name="my_datasource")
    data_asset = data_source.add_dataframe_asset(name="my_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("my_batch")

    validation_definition = gx.ValidationDefinition(
        data=batch_definition,
        suite=suite,
        name="ci_validation"
    )

    # 4. Çalıştır
    results = validation_definition.run(batch_parameters={"dataframe": df})

    # 5. Özet ve Slack Bildirimi
    # GitHub'ın bize sağladığı otomatik değişkenleri kullanalım
    repo_name = os.getenv('GITHUB_REPOSITORY', 'Unknown Repo')
    run_id = os.getenv('GITHUB_RUN_ID', '0')
    job_url = f"https://github.com/{repo_name}/actions/runs/{run_id}"

    if not results.success:
        summary = f"🚨 *DQ ALERT: Validation Failed!* 🚨\n\n*Repo:* {repo_name}\n*Detaylar:* <{job_url}|GitHub Actions Loglarını Görüntüle>"
        send_slack_notification(summary)
        print("❌ Hata Slack'e bildirildi.")
        sys.exit(1)
    else:
        # Başarılı olduğunda da kısa bir not istersen:
        # send_slack_notification(f"✅ DQ Passed: {repo_name}")
        print("✅ Her şey yolunda.")
        sys.exit(0)

if __name__ == "__main__":
    run_validation()