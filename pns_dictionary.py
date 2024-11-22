from json import JSONEncoder
from typing import Hashable
from dataclasses import dataclass, is_dataclass, asdict
import warnings

warnings.simplefilter(action="ignore", category=FutureWarning)

import pandas as pd


@dataclass
class Column:
    name: str
    old_name: str
    values_dict: dict[str, str]

    def parse_value(self, old_value: str) -> str:
        return self.values_dict.get(old_value, old_value)


@dataclass
class Module:
    """Representa subdivisões das colunas da base e serve como um dicionário
    para traduzir os dados da base em informações legíveis para humanos."""

    name: str
    columns: dict[str, Column]

    @staticmethod
    def from_dataframe(
        reference_series: pd.Series, dataframe: pd.DataFrame
    ) -> "dict[str, Module]":
        reference_tuple = tuple(
            (index, name) for index, name in reference_series.items()
        )

        name: str
        modules: dict[str, Module] = {}
        for i in range(len(reference_tuple) - 1):
            start, name = reference_tuple[i]
            name = name.strip()
            end = reference_tuple[i + 1][0]
            modules[name] = Module._from_dataframe(name, start, end, dataframe)

        start, name = reference_tuple[len(reference_tuple) - 1]
        name = name.strip()
        modules[name] = Module._from_dataframe(name, start, 5161, dataframe)

        return modules

    @staticmethod
    def _from_dataframe(
        name: str, start: Hashable, end: Hashable, dataframe: pd.DataFrame
    ) -> "Module":
        filtered_df = dataframe.loc[
            start:end, ["descrição", "Código\nda\nvariável", "Tipo ", "Descrição"]
        ]
        filtered_df = filtered_df[filtered_df["Descrição"] != "Não aplicável"]

        column = None
        columns = {}
        for new_column_name, old_column_name, old_value, new_value in filtered_df[
            ["descrição", "Código\nda\nvariável", "Tipo ", "Descrição"]
        ].itertuples(index=False, name=None):
            new_column_name: str = new_column_name.strip()
            old_column_name: str = old_column_name.strip()
            new_value = new_value.strip() if isinstance(new_value, str) else new_value

            try:
                old_value = int(old_value)
            except:
                pass
            finally:
                old_value = str(old_value).strip()

            if column is None:
                column = Column(
                    new_column_name, old_column_name, {old_value: new_value}
                )
                continue

            if column.name == new_column_name:
                column.values_dict[old_value] = new_value
                continue

            columns[column.old_name] = column
            column = Column(new_column_name, old_column_name, {old_value: new_value})

        if column is not None:
            columns[column.old_name] = column

        return Module(name, columns)

    def filter_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        column_names = list(map(lambda column: column.old_name, self.columns.values()))
        return df[column_names]

    def parse_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = [
            self.columns[column].name if column in self.columns else column
            for column in df.columns
        ]

        for column in self.columns.values():
            try:
                df.loc[:, column.name] = df[column.name].apply(
                    lambda v: column.parse_value(str(int(v))) if pd.notna(v) else v
                )
            except Exception as e:
                print(column.name)

        return df

    def get_module_dataframe(self, original_df: pd.DataFrame) -> pd.DataFrame:
        return self.parse_columns(self.filter_columns(original_df.loc[:]))


base = pd.read_excel("dicionario_PNS_microdados_2019.xls", header=1)
base.columns = [
    base[column][0] if not pd.isna(base[column][0]) else column
    for column in base.columns
]
base = base.drop(index=0).ffill()

modules_series = base[
    base["Posição inicial "].apply(
        lambda x: isinstance(x, str)
        and ("módulo" in x.casefold() or "modulo" in x.casefold())
    )
]["Posição inicial "]
dictionary = Module.from_dataframe(modules_series, base)
