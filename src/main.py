"""
Call zbozi categories API and get names and paths for categories specified in the input file.
"""

import csv
import logging
import os
import time
from collections.abc import Mapping, Iterable

import requests
import logging_gelf.formatters
import logging_gelf.handlers
from keboola import docker


def chunks(input_iterable, chunk_size):
    for i in range(0, len(input_iterable), chunk_size):
        yield input_iterable[i: i + chunk_size]


def get_categories_tree(url_base, authorization_tuple):
    response = requests.get(f"{url_base}/v1/categories/tree", auth=authorization_tuple)

    if response.status_code // 100 != 2:
        return None

    content = response.json()
    return content["data"]


def get_category_names(url_base, authorization_tuple, category_ids_str):
    response = requests.get(
        f"{url_base}/v1/categories/{category_ids_str}", auth=authorization_tuple
    )

    if response.status_code // 100 != 2:
        return None

    content = response.json()
    try:
        categories_info = []
        categories = content["data"]
        for category in categories:
            category_info = {
                "CATEGORY_ID": category["categoryId"],
                "CATEGORY_NAME": category["path"][-1],
                "CATEGORY_PATH": "|".join(category["path"]),
            }
            categories_info.append(category_info)
    except Exception:
        logging.error(f"No info returned for categories: {category_ids_str}")
        return []
    else:
        return categories_info


def main():
    # noinspection PyArgumentList
    logging.basicConfig(
        level=logging.DEBUG, handlers=[]
    )  # do not create default stdout handler
    logger = logging.getLogger()
    try:
        logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(
            host=os.getenv("KBC_LOGGER_ADDR"), port=int(os.getenv("KBC_LOGGER_PORT"))
        )
    except TypeError:
        logging_gelf_handler = logging.StreamHandler()

    logging_gelf_handler.setFormatter(
        logging_gelf.formatters.GELFFormatter(null_character=True)
    )
    logger.addHandler(logging_gelf_handler)
    logger.setLevel(logging.INFO)

    datadir = os.getenv("KBC_DATADIR", "/data/")
    conf = docker.Config(datadir)
    params = conf.get_parameters()
    logger.info("Extracted parameters.")
    logger.info({k: v for k, v in params.items() if "#" not in k})

    api_url_base = params.get("api_url")
    sleep_time = int(params.get("sleep_time"))
    chunk_size = int(params.get("chunk_size"))
    login = int(params.get("login"))
    password = params.get("#password")
    auth_tuple = (login, password)

    logger.info(f"Getting category ids.")
    category_tree = get_categories_tree(api_url_base, auth_tuple)

    leafs_to_explore = category_tree
    ids = list()

    while len(leafs_to_explore) > 0:
        for leaf in leafs_to_explore.copy():
            if "categoryId" in leaf.keys():
                ids.append(leaf["categoryId"])
            if leaf.get("children") is not None:
                leafs_to_explore = leafs_to_explore + leaf["children"]
            leafs_to_explore.remove(leaf)
        logger.info(f" Remaining leafs to explore: {(len(leafs_to_explore))}")

    categories = {str(cat_id) for cat_id in ids}
    logger.info(f"Collected {len(categories)} category ids.")

    with open(f"{datadir}out/tables/results.csv", "wt") as outfile:
        dict_writer = csv.DictWriter(
            outfile, fieldnames=["CATEGORY_ID", "CATEGORY_NAME", "CATEGORY_PATH"]
        )
        dict_writer.writeheader()
        for batch in chunks(list(categories), chunk_size=chunk_size):
            category_ids_str = ",".join(batch)
            logger.info(f"Getting names for ids: {category_ids_str}")
            batch_names = get_category_names(
                url_base=api_url_base,
                authorization_tuple=auth_tuple,
                category_ids_str=category_ids_str,
            )
            if isinstance(batch_names, Iterable):
                dict_writer.writerows(batch_names)
            elif isinstance(batch_names, Mapping):
                dict_writer.writerow(batch_names)
            else:
                logger.error("Batch result is neither Mapping nor Iterable.")
                logger.info(f"Batch names: {batch_names}")
                raise ValueError
            time.sleep(sleep_time)


if __name__ == "__main__":
    main()
