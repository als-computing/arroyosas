from arroyo_reduction.operator import LatentSpaceOperator
from arroyo_reduction.reducer import LatentSpaceReducer, Reducer
from arroyo_reduction.schemas import LatentSpaceEvent 


def build_lse_operator() -> LatentSpaceOperator:
    reducer = LatentSpaceReducer()
    return LatentSpaceOperator(reducer)
