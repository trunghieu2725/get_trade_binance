from pathlib import Path

import yaml

from quality.framework.models import Rule


class RuleRegistry:

    def __init__(self, config_path: str):

        self.config_path = Path(config_path)

    def load_rules(self) -> list[Rule]:

        if not self.config_path.exists():

            raise FileNotFoundError(self.config_path)

        with open(self.config_path, "r", encoding="utf-8") as f:

            config = yaml.safe_load(f)

        rules = []

        for item in config["rules"]:

            if not item.get("enabled", True):

                continue

            rule = Rule(

                id=item["id"],

                name=item["name"],

                description=item["description"],

                enabled=item["enabled"],

                layer=item["layer"],

                severity=item["severity"],

                owner=item["owner"],

                type=item["type"],

                query=item["query"],

                metric=item["metric"],

                operator=item["operator"],

                threshold=item["threshold"],

                tags=item.get("tags", [])

            )

            rules.append(rule)

        return rules