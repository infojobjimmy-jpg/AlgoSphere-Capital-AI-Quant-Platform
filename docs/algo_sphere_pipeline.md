# Algo Sphere Pipeline

## End-to-End Lifecycle
1. **Generate**  
   Bot Factory produces new strategy candidates with baseline parameters and initial fitness.

2. **Evolve**  
   Evolution Engine mutates/crosses top candidates to explore improved variants.

3. **Paper Test**  
   Paper Trading Engine runs simulation metrics only.

4. **Feedback**  
   Feedback Engine scores paper outcomes and decides promote/reject/evolve-again.

5. **Live Safe**  
   Live Safe Mode classifies candidates for safe progression review.

6. **Review Desk**  
   Candidate Review Desk enables explicit human approval/rejection workflows.

7. **Demo Deploy**  
   Demo Deploy Desk manages queue/assignment/pause/reject for controlled demo preparation.

8. **Portfolio Allocation**  
   Portfolio AI assigns capital weights; Capital Engine simulates resulting utilization.

## Key States
- `GENERATED`: initial candidate from factory.
- `MUTATED`: evolved variant from top performers.
- `PAPER_RUNNING`: currently in simulated paper run.
- `PAPER_SUCCESS`: passed paper criteria.
- `APPROVED_FOR_LIVE_REVIEW`: safe-mode progression candidate.
- `LIVE_SAFE_READY`: strong safe candidate ready for human gating.
- `DEMO_QUEUE`: accepted into demo queue workflow (still non-executing).

## Human-in-the-Loop Gates
- Review Desk gate: explicit operator decision before demo progression.
- Demo Deploy Desk gate: queue and assignment management without execution rights.
- Operator Console gate: centralized oversight of readiness and risk posture.
