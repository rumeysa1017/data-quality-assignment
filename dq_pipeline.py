import pandas as pd
import great_expectations as gx
import sys

def run_validation():
    # 1. Veriyi Yükle
    df = pd.read_csv("data/amazon_orders.csv", low_memory=False)
    
    # 2. GE Context ve Suite Ayarları
    context = gx.get_context()
    suite = context.suites.add(gx.ExpectationSuite(name="ci_suite"))
    
    # 3. Beklentileri Ekle (Basit tutalım CI hızlı çalışsın)
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="Order ID"))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="Qty", min_value=0))

    # 4. Datasource ve Batch Tanımı
    data_source = context.data_sources.add_pandas(name="my_datasource")
    data_asset = data_source.add_dataframe_asset(name="my_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("my_batch")

    validation_definition = gx.ValidationDefinition(
        data=batch_definition,
        suite=suite,
        name="ci_validation"
    )

    # 5. Çalıştır
    results = validation_definition.run(batch_parameters={"dataframe": df})

    # 6. SONUÇ KONTROLÜ (CI için en kritik yer)
    if not results.success:
        print("❌ Veri Kalitesi Doğrulaması Başarısız!")
        # GitHub'a "Hata var!" mesajı gönderiyoruz
        sys.exit(1) 
    else:
        print("✅ Veri Kalitesi Doğrulaması Başarılı.")
        sys.exit(0)

if __name__ == "__main__":
    run_validation()