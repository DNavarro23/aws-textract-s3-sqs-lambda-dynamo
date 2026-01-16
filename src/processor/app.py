import os
import json
import time
import urllib.parse
import boto3

textract = boto3.client("textract")
s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")

TABLE_NAME = os.environ["DDB_TABLE"]
OUTPUT_BUCKET = os.environ["OUTPUT_BUCKET"]
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "processed/")

table = ddb.Table(TABLE_NAME)

def _extract_text_lines(textract_response: dict) -> str:
    blocks = textract_response.get("Blocks", [])
    lines = [b["Text"] for b in blocks if b.get("BlockType") == "LINE" and "Text" in b]
    return "\n".join(lines)

def lambda_handler(event, context):
    # event viene de SQS. Cada record.body contiene un evento de S3 (JSON).
    for record in event.get("Records", []):
        body_str = record.get("body", "{}")
        body = json.loads(body_str)

        for s3rec in body.get("Records", []):
            bucket = s3rec["s3"]["bucket"]["name"]
            key = urllib.parse.unquote_plus(s3rec["s3"]["object"]["key"])
            doc_id = f"{bucket}/{key}"
            now = int(time.time())

            try:
                # TEXTRACT SINCRONO:
                # - Funciona muy bien para IMAGENES (png/jpg) o PDFs pequeños.
                # - Para PDFs grandes/multipagina normalmente se usa Textract ASINCRONO (lo podemos extender luego).
                resp = textract.analyze_document(
                    Document={"S3Object": {"Bucket": bucket, "Name": key}},
                    FeatureTypes=["TABLES", "FORMS"],
                )

                text = _extract_text_lines(resp)

                # Guardar respuesta cruda en S3 de salida:
                filename = key.split("/")[-1]
                out_key = f"{OUTPUT_PREFIX}{filename}.textract.json"

                s3.put_object(
                    Bucket=OUTPUT_BUCKET,
                    Key=out_key,
                    Body=json.dumps(resp).encode("utf-8"),
                    ContentType="application/json",
                )

                # Guardar resultado/resumen en DynamoDB:
                table.put_item(
                    Item={
                        "DocumentId": doc_id,
                        "Status": "DONE",
                        "SourceBucket": bucket,
                        "SourceKey": key,
                        "OutputBucket": OUTPUT_BUCKET,
                        "OutputKey": out_key,
                        "CreatedAtEpoch": now,
                        "TextPreview": text[:1000],
                    }
                )

            except Exception as e:
                table.put_item(
                    Item={
                        "DocumentId": doc_id,
                        "Status": "ERROR",
                        "SourceBucket": bucket,
                        "SourceKey": key,
                        "CreatedAtEpoch": now,
                        "Error": str(e)[:1000],
                    }
                )
                # Si levantamos error, SQS reintentará y luego puede caer a la DLQ.
                raise

    return {"ok": True}

