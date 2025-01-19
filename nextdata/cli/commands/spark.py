import asyncclick as click
import asyncio

from nextdata.core.pulumi_context_manager import PulumiContextManager
from nextdata.core.connections.spark import SparkManager


@click.group()
def spark():
    pass


@spark.command(name="session")
async def create_session():
    pulumi_context_manager = PulumiContextManager()
    pulumi_context_manager.initialize_stack()
    spark_manager = SparkManager()

    # Initialize variables in global namespace
    globals().update(
        {
            "pulumi_context_manager": pulumi_context_manager,
            "spark": spark_manager,
        }
    )

    # Start IPython shell for better tab completion
    try:
        import IPython
        import nest_asyncio

        # Apply nest_asyncio to allow running async code in IPython
        nest_asyncio.apply()

        IPython.embed(
            banner1=f"NextData Spark Session\nAvailable objects:\n- spark: SparkManager\n- stack_outputs: StackOutputs\n- pulumi_context_manager: PulumiContextManager",
            colors="neutral",
        )
    except ImportError:
        # Fallback to regular Python shell if IPython not available
        import code

        code.interact(
            banner=f"NextData Spark Session\nAvailable objects:\n- spark: SparkManager\n- stack_outputs: StackOutputs\n- pulumi_context_manager: PulumiContextManager",
            local=globals(),
        )
