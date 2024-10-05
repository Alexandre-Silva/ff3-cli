import csv
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Self, Any

from pathlib import Path
import pydantic as pd
import requests
import toml
import typer
from pydantic import dataclasses as pdd


@dataclass()
class Client:
    url: str
    token: str

    fail: bool = True

    @classmethod
    def from_toml(cls, path: str) -> Self:
        return cls(**toml.load(path)["server"])

    def get(self, path, params=None):
        res = requests.get(
            f"{self.url}/api/v1/{path}", headers=self._headers(), params=params
        )

        if self.fail:
            res.raise_for_status()

        return res.json()

    def get_paged(self, path, params=None):
        p = params or {}
        out = []
        for i in range(1, 100):
            res = self.get(path, {**p, "page": i})
            out.extend(res["data"])

            if not res["meta"]["pagination"]["total_pages"] == i:
                break

        return out

    def post(self, path, body):
        url = f"{self.url}/api/v1/{path}"
        res = requests.post(url, headers=self._headers(), json=body)

        if self.fail:
            try:
                print(res.json())
            except json.decoder.JSONDecodeError:
                print(res.text)

            res.raise_for_status()

        return res.json()

    def delete(self, path):
        url = f"{self.url}/api/v1/{path}"
        res = requests.delete(url, headers=self._headers())

        if self.fail:
            try:
                print(res.json())
            except json.decoder.JSONDecodeError:
                print(res.text)

            res.raise_for_status()

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.api+json",
        }


@dataclass()
class FF3Operator:
    client: Client

    _accounts_raw: list[dict] = field(default_factory=list)
    _accounts: list["FF3Account"] = field(default_factory=list)

    def account_fetch(self):
        self._accounts_raw = self.client.get_paged("accounts")
        self.account_convert()

    def account_convert(self):
        adapter: pd.TypeAdapter[FF3Account] = pd.TypeAdapter(FF3Account)

        for d in self._accounts_raw:
            notes: str | None = d["attributes"].get("notes")
            meta = {}

            if notes and "--meta--" in notes:
                meta_raw = []
                notes_l = notes.splitlines()
                in_meta = False
                for l in notes_l:
                    if "--meta--" in l:
                        in_meta = True
                        continue
                    elif in_meta:
                        meta_raw.append(l)
                    elif "--meta-end--" in l:
                        break

                if notes_l:
                    meta = json.loads("".join(meta_raw))

                    print(f"{notes} -> {meta}")

            acc = adapter.validate_python(d["attributes"])
            acc.id_ = d["id"]
            acc._meta = meta

            self._accounts.append(acc)

    def account_list(self):
        if len(self._accounts_raw) == 0:
            self.account_fetch()
        return self._accounts_raw

    def account_del_imported(self):
        if len(self._accounts_raw) == 0:
            self.account_fetch()

        for acc in self._accounts_raw:
            notes = acc["attributes"].get("notes")
            if notes is not None and "Imported from GnuCash" in notes:
                print(f"Deleting account: {acc['id']} {acc['attributes']['name']}")
                self.client.delete(f"accounts/{acc['id']}")
            else:
                print(f"Skipping account: {acc['id']} {acc['attributes']['name']}")

    def account_create(self, acc: "FF3Account"):
        return self.client.post("accounts", acc.to_dict())


@dataclass()
class GCTranslator:
    accounts: list["GCAccount"] = field(default_factory=list)
    account_map: dict["GCAccount", "FF3Account"] = field(default_factory=dict)

    def load_accounts_csv(self, path):
        with open(path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                account = GCAccount(**row)
                self.accounts.append(account)

    def convert_account(self, acc_gc: "GCAccount") -> "FF3Account|None":
        typeMap = {
            "ASSET": "asset",
            "CASH": "asset",
            "BANK": "asset",
            "EXPENSE": "expense",
            "INCOME": "revenue",
            "LIABILITY": "liability",
            "CREDIT": "asset",
        }
        roleMap = {
            "ASSET": "defaultAsset",
            "CASH": "cashWalletAsset",
            "BANK": "defaultAsset",
            "EXPENSE": "expense",
            "INCOME": "revenue",
            "LIABILITY": "liability",
            "CREDIT": "ccAsset",
        }

        if acc_gc.name in (
            "Assets",
            "Liabilities",
            "Liabilities",
            "Income",
            "Expenses",
            "Current Assets",
            "Credit Card",
        ):
            print(f"Skipping account: {acc_gc.name}")
            return None

        if acc_gc.type_ == "EQUITY":
            return None

        new_type = typeMap[acc_gc.type_]

        subname = acc_gc.name_full.split(":")
        if len(subname) <= 1:
            print(f"Skipping account: {acc_gc.name}")
            return None

        name = ":".join(subname[1:])

        meta = json.dumps({"name": acc_gc.name_full})

        description = f"""{acc_gc.description}

Imported from GnuCash

--meta--
{meta}
--meta-end--
"""

        if new_type == "asset":
            acc_ff3 = FF3Account(
                name=name,
                type_=new_type,
                currency_code=acc_gc.symbol,
                role=roleMap[acc_gc.type_],
                notes=description,
            )

        elif new_type == "liability":
            raise NotImplementedError()

        else:
            acc_ff3 = FF3Account(
                name=name,
                type_=new_type,
                currency_code=acc_gc.symbol,
                notes=description,
            )

        assert acc_gc not in self.account_map
        self.account_map[acc_gc] = acc_ff3

        return acc_ff3


@pdd.dataclass()
class GCAccount:
    type_: str = pd.Field(alias="Type")
    name_full: str = pd.Field(alias="Full Account Name")
    name: str = pd.Field(alias="Account Name")
    code: str = pd.Field(alias="Account Code")
    description: str = pd.Field(alias="Description")
    color: str = pd.Field(alias="Account Color")
    notes: str = pd.Field(alias="Notes")
    symbol: str = pd.Field(alias="Symbol")  # currency
    namespace: str = pd.Field(alias="Namespace")
    hidden: str = pd.Field(alias="Hidden")
    tax_info: str = pd.Field(alias="Tax Info")
    placeholder: str = pd.Field(alias="Placeholder")

    def __hash__(self):
        return hash(self.name_full)


@pdd.dataclass()
class FF3Account:
    type_: str = pd.Field(alias="type")
    id_: int = pd.Field(default=-1, exclude=True)
    name: str = pd.Field(default="")
    role: str | None = pd.Field(default=None, alias="account_role")
    currency_code: str = pd.Field(default="EUR")
    include_net_worth: bool = pd.Field(default=True)
    notes: str | None = pd.Field(default="")
    _meta: dict[str, Any] = pd.Field(default_factory=dict, exclude=True)

    def to_dict(self):
        adapter = pd.TypeAdapter(FF3Account)
        data = adapter.dump_python(
            self,
            by_alias=True,
            exclude_none=True,
        )
        return data


@dataclass
class FF3CLI:
    t: typer.Typer = field(default_factory=typer.Typer)
    out_format: str = "text"

    def __post_init__(self):
        self.client = Client.from_toml("config.toml")
        self.op = FF3Operator(self.client)


cli = FF3CLI()


class OutFormat(str, Enum):
    py = "py"
    json = "json"


class AccountGroup(str, Enum):
    all = "all"
    imported = "imported"


@cli.t.command()
def account_list(fmt: OutFormat = OutFormat.py, raw: bool = True):
    out = cli.op.account_list()
    if fmt == OutFormat.py:
        print(out)
    elif fmt == OutFormat.json:
        print(json.dumps(out))


@cli.t.command()
def account_create():
    return cli.client.post("accounts", acc.to_dict())


@cli.t.command()
def account_delete(group: AccountGroup):
    if group == AccountGroup.imported:
        return cli.op.account_del_imported()
    else:
        print("Not implemented")
        raise typer.Exit(code=1)


class ImportType(str, Enum):
    gc_accounts = "gnucash-accounts"
    gc_transactions = "gnucash-transactions"


@cli.t.command("import")
def import_(type_: ImportType, file: Path, do_clear: bool = True):
    """
    --do-clear: Clear previously imported data
    """

    if type_ == ImportType.gc_accounts:
        if do_clear:
            cli.op.account_del_imported()

        gc = GCTranslator()
        gc.load_accounts_csv(file)
        for acc in gc.accounts:
            ff3_acc = gc.convert_account(acc)
            if ff3_acc:
                cli.op.account_create(ff3_acc)
                # callApi("accounts", body=ff3_acc.dict())

    else:
        print("Not implemented")
        raise typer.Exit(code=1)


def dbg(self):
    c, op = self.client, self.op
    if False:
        print(json.dumps(op.account_list()))
        return

    if True:
        op.account_del_imported()
    return

    gc = GCTranslator()
    gc.load_accounts_csv("accounts.csv")
    for acc in gc.accounts:
        ff3_acc = gc.convert_account(acc)
        if ff3_acc:
            print(ff3_acc)
            ff3.account_create(ff3_acc)
            # callApi("accounts", body=ff3_acc.dict())


if __name__ == "__main__":
    cli.t()
