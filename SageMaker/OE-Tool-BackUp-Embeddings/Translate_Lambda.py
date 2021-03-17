import json
import boto3
translate = boto3.client("translate")

def lambda_handler(event, context):
    
    payload = event['data']
    source_language_code = event['source_language_code']
    
    translation = translate.translate_text(
        Text = payload,
        SourceLanguageCode = source_language_code,#"ja",
        TargetLanguageCode = "en"
    )['TranslatedText']

    return translation

    # Translation Lambda 
    # aws-translate-api