import logging

from dags import httpclient


def extract_barcodes(id: int, writer, host: str, token: str) -> None:
    r"""
    Extract barcodes from 1C
    """
    url = f"{host}/items/barcodes"
    headers = {"Key": f"{token}"}
    req = httpclient.get(url=url, headers=headers)
    req.raise_for_status()
    data = req.json()
    items = data["items"]
    size = len(items)
    if size == 0:
        logging.info(f"Data is empty, skipping")
        return None
    index = 0
    for item in items:
        writer.writerow([
            item["id"],
            item["barcodeID"],
            item["barcode"],
            item["orgId"],
            item["marketId"],
            item["article"].replace('"','').strip(),
            id
        ])
        index += 1
    logging.info(f"Finished {index}")
