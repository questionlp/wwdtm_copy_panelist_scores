# -*- coding: utf-8 -*-
# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2018-2023 Linh Pham
# wwdtm-copy-panelist-scores is released under the terms of the Apache License 2.0
"""Copy existing panelist score value into new panelist score decimal column
as part of a migration from wwdtm database version 4.1 to 4.2"""

import json

import mysql.connector


def load_config():
    """Load configuration settings from config.json"""
    with open("config.json", "r", encoding="utf-8") as config_file:
        config_dict = json.load(config_file)

    return config_dict


def copy_panelist_lightning_start(database_connection: mysql.connector.connect) -> None:
    """Copies the panelistlrndstart value for each entry in the
    ww_showpnlmap table into the panelistlrndstart_decimal column"""

    query = "SHOW COLUMNS FROM ww_showpnlmap WHERE Field = 'panelistlrndstart_decimal';"
    cursor = database_connection.cursor()
    cursor.execute(query)
    start_decimal = cursor.fetchone()

    if not start_decimal:
        return

    cursor = database_connection.cursor(named_tuple=True)
    query = """
    SELECT showpnlmapid, panelistlrndstart, panelistlrndstart_decimal
    FROM ww_showpnlmap
    WHERE panelistlrndstart IS NOT NULL
    AND panelistlrndstart_decimal IS NULL;
    """

    cursor.execute(query)
    result = cursor.fetchall()

    if result:
        for row in result:
            _id = row.showpnlmapid
            score = row.panelistlrndstart
            query = """
            UPDATE ww_showpnlmap SET panelistlrndstart_decimal = %s
            WHERE showpnlmapid = %s;
            """
            cursor.execute(
                query,
                (
                    score,
                    _id,
                ),
            )

    cursor.close()


def copy_panelist_lightning_correct(database_connection: mysql.connector.connect) -> None:
    """Copies the panelistlrndcorrect value for each entry in the
    ww_showpnlmap table into the panelistlrndcorrect_decimal column"""

    query = "SHOW COLUMNS FROM ww_showpnlmap WHERE Field = 'panelistlrndcorrect_decimal';"
    cursor = database_connection.cursor()
    cursor.execute(query)
    correct_decimal = cursor.fetchone()

    if not correct_decimal:
        return

    cursor = database_connection.cursor(named_tuple=True)
    query = """
    SELECT showpnlmapid, panelistlrndcorrect, panelistlrndcorrect_decimal
    FROM ww_showpnlmap
    WHERE panelistlrndcorrect IS NOT NULL
    AND panelistlrndcorrect_decimal IS NULL;
    """

    cursor.execute(query)
    result = cursor.fetchall()

    if result:
        for row in result:
            _id = row.showpnlmapid
            score = row.panelistlrndcorrect
            query = """
            UPDATE ww_showpnlmap SET panelistlrndcorrect_decimal = %s
            WHERE showpnlmapid = %s;
            """
            cursor.execute(
                query,
                (
                    score,
                    _id,
                ),
            )

    cursor.close()


def copy_panelist_scores(database_connection: mysql.connector.connect) -> None:
    """Copies the panelistscore value for each entry in the
    ww_showpnlmap table into the panelistscore_decimal column"""

    query = "SHOW COLUMNS FROM ww_showpnlmap WHERE Field = 'panelistscore_decimal';"
    cursor = database_connection.cursor()
    cursor.execute(query)
    score_decimal = cursor.fetchone()

    if not score_decimal:
        return

    cursor = database_connection.cursor(named_tuple=True)
    query = """
    SELECT showpnlmapid, panelistscore, panelistscore_decimal
    FROM ww_showpnlmap
    WHERE panelistscore IS NOT NULL
    AND panelistscore_decimal IS NULL;
    """

    cursor.execute(query)
    result = cursor.fetchall()

    if result:
        for row in result:
            _id = row.showpnlmapid
            score = row.panelistscore
            query = """
            UPDATE ww_showpnlmap SET panelistscore_decimal = %s
            WHERE showpnlmapid = %s;
            """
            cursor.execute(
                query,
                (
                    score,
                    _id,
                ),
            )

    cursor.close()


def main() -> None:
    """Copy panelist scores into new column"""
    config = load_config()
    database_connection = mysql.connector.connect(**config["database"])
    copy_panelist_lightning_start(database_connection=database_connection)
    copy_panelist_lightning_correct(database_connection=database_connection)
    copy_panelist_scores(database_connection=database_connection)


if __name__ == "__main__":
    main()
