from osprey.engine.executor.execution_context import ExecutionContext
from osprey.engine.udf.arguments import ArgumentsBase
from osprey.engine.udf.base import UDFBase


class DomainBaseArguments(ArgumentsBase):
    domain: str


class RootDomain(UDFBase[DomainBaseArguments, str]):
    def execute(self, execution_context: ExecutionContext, arguments: DomainBaseArguments) -> str:
        return get_root_domain(arguments.domain)


def get_root_domain(domain: str) -> str:
    pts = domain.split('.')
    if len(pts) == 1:
        return pts[0]
    return f'{pts[len(pts) - 2]}.{pts[len(pts) - 1]}'
