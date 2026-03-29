from dataclasses import dataclass, field

from .genetic import GeneticTuner


@dataclass
class EvolutionEngine:
    tuner: GeneticTuner = field(default_factory=GeneticTuner)

    def evolve_threshold(self, current_threshold: float, improved: bool) -> float:
        direction = 1 if improved else -1
        return self.tuner.mutate(current_threshold, direction=direction)
