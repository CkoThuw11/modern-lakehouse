"""DBT Spark Operator for Airflow — Docker Compose edition.

This adapts the clean, configurable "DbtSparkOperator" architecture (dataclass
config, one command-building operator, specialized per-command subclasses,
``template_fields``, structured logging) to THIS stack, which is Docker Compose
rather than Kubernetes:

- No ``KubernetesPodOperator`` / no per-task pods — dbt is baked into the Airflow
  image at ``/opt/dbt-venv`` (see ``docker/airflow/Dockerfile``) and executed in
  a subprocess. The two dependency trees (Airflow vs dbt) stay isolated.
- No Kyuubi — dbt-spark connects over thrift to the Spark Thrift server
  (``spark:10000``) using the committed ``dbt/profiles.yml``, whose host/port read
  from ``SPARK_THRIFT_HOST`` / ``SPARK_THRIFT_PORT`` (set here from the config).
- dbt writes ``target/``/``logs/`` under ``/tmp`` so it never needs write access to
  the host-mounted, uid-mismatched project dir.

The public surface (``DbtSparkConfig`` + ``DbtSpark*Operator`` classes) mirrors the
Kubernetes version so DAGs read the same way.
"""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.context import Context


@dataclass
class DbtSparkConfig:
    """Configuration for dbt execution against the Spark Thrift server."""

    # dbt project (bind-mounted into the Airflow image) + its committed profiles.yml.
    dbt_project_dir: str = "/opt/airflow/dbt"
    dbt_profiles_dir: str = "/opt/airflow/dbt"
    # dbt CLI from the dedicated venv baked into the image (never Airflow's own env).
    dbt_bin: str = "/opt/dbt-venv/bin/dbt"

    # Spark Thrift server (dbt-spark thrift method). profiles.yml reads these from env.
    spark_host: str = "spark"
    spark_port: int = 10000

    # Keep run artifacts/logs off the read-mostly, uid-mismatched project mount.
    target_path: str = "/tmp/dbt-target"
    log_path: str = "/tmp/dbt-logs"

    threads: int = 2
    extra_env_vars: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "DbtSparkConfig":
        """Build config from the container environment (same vars the stack uses)."""
        return cls(
            dbt_project_dir=os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt"),
            dbt_profiles_dir=os.environ.get("DBT_PROFILES_DIR", "/opt/airflow/dbt"),
            dbt_bin=os.path.join(os.environ.get("DBT_VENV", "/opt/dbt-venv"), "bin", "dbt"),
            spark_host=os.environ.get("SPARK_THRIFT_HOST", "spark"),
            spark_port=int(os.environ.get("SPARK_THRIFT_PORT", "10000")),
        )


class DbtSparkOperator(BaseOperator):
    """Run a single dbt command against Spark Thrift, in a subprocess.

    Configurable via :class:`DbtSparkConfig` (typically passed through
    ``default_args`` so every dbt task in a DAG shares it). Specialized
    subclasses below fix ``command`` for the common dbt verbs.
    """

    template_fields: Sequence[str] = ("command", "select", "exclude", "vars", "target")
    ui_color = "#FF6B35"
    ui_fgcolor = "#FFFFFF"

    def __init__(
        self,
        command: str,
        select: Optional[str] = None,
        exclude: Optional[str] = None,
        vars: Optional[Dict[str, Any]] = None,
        target: str = "dev",
        full_refresh: bool = False,
        threads: Optional[int] = None,
        # Accepted for API parity with the Kubernetes/Kyuubi operator; the Spark
        # Thrift server here is a single local[*] engine, so per-task executor
        # counts don't apply — logged and ignored rather than silently dropped.
        num_executors: Optional[int] = None,
        config: Optional[DbtSparkConfig] = None,
        extra_env: Optional[Dict[str, str]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.command = command
        self.select = select
        self.exclude = exclude
        self.vars = vars or {}
        self.target = target
        self.full_refresh = full_refresh
        self.config = config or DbtSparkConfig.from_env()
        self.threads = threads or self.config.threads
        self.num_executors = num_executors
        self.extra_env = extra_env or {}

    # -- command assembly ---------------------------------------------------

    def _command_supports_vars(self) -> bool:
        return self._verb in {"run", "test", "compile", "seed", "snapshot"}

    def _command_supports_full_refresh(self) -> bool:
        return self._verb in {"run", "seed"}

    def _command_supports_threads(self) -> bool:
        return self._verb in {"run", "test", "compile", "seed", "snapshot"}

    @property
    def _verb(self) -> str:
        """First word of the command (``docs generate`` → ``docs``)."""
        return self.command.split()[0]

    def _build_command(self) -> List[str]:
        # command may be multi-word ("docs generate", "source freshness").
        cmd: List[str] = [self.config.dbt_bin, *self.command.split()]
        cmd += ["--target", self.target]
        cmd += ["--profiles-dir", self.config.dbt_profiles_dir]
        cmd += ["--project-dir", self.config.dbt_project_dir]
        # target/log paths are set via env vars (DBT_TARGET_PATH/DBT_LOG_PATH) in
        # _build_env, not CLI flags — `dbt debug`/`deps` reject `--target-path`.

        if self.select:
            cmd += ["--select", self.select]
        if self.exclude:
            cmd += ["--exclude", self.exclude]
        if self.vars and self._command_supports_vars():
            vars_str = ", ".join(f"{k}: {v}" for k, v in self.vars.items())
            cmd += ["--vars", "{" + vars_str + "}"]
        if self.full_refresh and self._command_supports_full_refresh():
            cmd.append("--full-refresh")
        if self._command_supports_threads():
            cmd += ["--threads", str(self.threads)]
        return cmd

    def _build_env(self) -> Dict[str, str]:
        env = os.environ.copy()
        # profiles.yml resolves host/port from these.
        env["SPARK_THRIFT_HOST"] = self.config.spark_host
        env["SPARK_THRIFT_PORT"] = str(self.config.spark_port)
        env["DBT_PROFILES_DIR"] = self.config.dbt_profiles_dir
        # Keep run artifacts/logs off the host-mounted, uid-mismatched project dir.
        # Env vars apply to every dbt command (unlike the --target-path CLI flag).
        env["DBT_TARGET_PATH"] = self.config.target_path
        env["DBT_LOG_PATH"] = self.config.log_path
        env.update(self.config.extra_env_vars)
        env.update(self.extra_env)
        return env

    # -- execution ----------------------------------------------------------

    def execute(self, context: Context) -> None:
        if self.num_executors is not None:
            self.log.info(
                "num_executors=%s ignored: Spark Thrift here is a single local[*] "
                "engine (no per-task executor scaling).",
                self.num_executors,
            )

        cmd = self._build_command()
        self.log.info("Running dbt: %s", " ".join(cmd))

        # Stream child output line-by-line into the Airflow task log.
        process = subprocess.Popen(
            cmd,
            cwd=self.config.dbt_project_dir,
            env=self._build_env(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert process.stdout is not None
        for line in process.stdout:
            self.log.info(line.rstrip())
        returncode = process.wait()

        if returncode != 0:
            raise AirflowException(
                f"dbt {self.command} failed with exit code {returncode}"
            )
        self.log.info("dbt %s completed successfully", self.command)


# -- specialized operators (fixed dbt verb) --------------------------------


class DbtSparkRunOperator(DbtSparkOperator):
    """``dbt run`` with optional model selection."""

    def __init__(self, models: Optional[Union[str, List[str]]] = None, **kwargs) -> None:
        if models and "select" not in kwargs:
            kwargs["select"] = " ".join(models) if isinstance(models, list) else models
        super().__init__(command="run", **kwargs)


class DbtSparkTestOperator(DbtSparkOperator):
    """``dbt test``."""

    def __init__(self, **kwargs) -> None:
        super().__init__(command="test", **kwargs)


class DbtSparkDebugOperator(DbtSparkOperator):
    """``dbt debug`` — validates the Spark Thrift connection + project config."""

    def __init__(self, **kwargs) -> None:
        super().__init__(command="debug", **kwargs)


class DbtSparkDepsOperator(DbtSparkOperator):
    """``dbt deps``."""

    def __init__(self, **kwargs) -> None:
        super().__init__(command="deps", **kwargs)


class DbtSparkSeedOperator(DbtSparkOperator):
    """``dbt seed``."""

    def __init__(self, **kwargs) -> None:
        super().__init__(command="seed", **kwargs)


class DbtSparkSnapshotOperator(DbtSparkOperator):
    """``dbt snapshot``."""

    def __init__(self, **kwargs) -> None:
        super().__init__(command="snapshot", **kwargs)


class DbtSparkCompileOperator(DbtSparkOperator):
    """``dbt compile``."""

    def __init__(self, **kwargs) -> None:
        super().__init__(command="compile", **kwargs)


class DbtSparkDocsOperator(DbtSparkOperator):
    """``dbt docs generate``."""

    def __init__(self, **kwargs) -> None:
        super().__init__(command="docs generate", **kwargs)
