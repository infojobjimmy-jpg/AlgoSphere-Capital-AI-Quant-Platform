from dataclasses import dataclass


@dataclass
class GeneticTuner:
    """
    Simple parameter tuner placeholder.
    """

    mutation_step: float = 0.05

    def mutate(self, value: float, direction: int = 1) -> float:
        return round(value + (self.mutation_step * direction), 4)
