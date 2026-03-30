from pyspark.sql.functions import col, concat_ws, sha2, lit
from utils.db import WATERMARK_SECRET_KEY

def apply_cryptographic_watermark(df):
    """
    Embeds a SHA-256 cryptographic watermark into every row to guarantee 
    data integrity, detect modification, and prove ownership.
    """
    columns_to_hash = [c for c in df.columns if c != "row_fingerprint"]

    # Append the WATERMARK_SECRET_KEY at the end as a cryptographic "Salt".
    row_payload = concat_ws("||", *[col(c).cast("string") for c in columns_to_hash], lit(WATERMARK_SECRET_KEY))

    # Apply the SHA-256 hashing algorithm to the payload to create the signature
    watermarked_df = df.withColumn("row_fingerprint", sha2(row_payload, 256))

    return watermarked_df

# Verify watermark integrity and detect tampering
def verify_data_integrity(df):
    """
    Recalculates fingerprints and identifies tampered rows.
    """
    columns_to_hash = [c for c in df.columns if c != "row_fingerprint"]
    
    # Recalculate what the hash SHOULD be
    recalculated_payload = concat_ws("||", *[col(c).cast("string") for c in columns_to_hash], lit(WATERMARK_SECRET_KEY))
    
    # Compare stored fingerprint vs recalculated fingerprint
    result_df = df.withColumn("is_authentic", col("row_fingerprint") == sha2(recalculated_payload, 256))
    
    tampered_count = result_df.filter(col("is_authentic") == False).count()
    
    if tampered_count > 0:
        print(f"ALERT: {tampered_count} rows have been tampered with!")
    else:
        print("SUCCESS: All data rows are authentic and unmodified.")
        
    return result_df