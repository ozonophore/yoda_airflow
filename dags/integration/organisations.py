import logging

from dags import httpclient


def extract_orgs(id: int, writer, host: str, token: str) -> None:
    r"""
    Extract orgs from 1C
    """
    url = f"{host}/organizations"
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
            item["name"],
            item["inn"],
            item["kpp"],
            item["updateAt"],
            id
        ])
        index += 1
    logging.info(f"Finished {index}")