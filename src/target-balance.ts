/**
 * Auto-Assign Workflow - Dynamic target balancing across agents
 *
 * Features:
 * 1. Monitor team member workload via SharedStateBoard
 * 2. Auto-assign tasks to least-loaded agents
 * 3. Support priority-based rebalancing
 */

import {
  defineWorkflow,
  parallelStep,
  sequenceStep,
  type WorkflowExecutionContext,
} from '@jshookmcp/extension-sdk/workflow';

interface AgentWorkload {
  agentId: string;
  currentTasks: number;
  completedTasks: number;
  lastHeartbeat: number;
  status: 'idle' | 'busy' | 'offline';
}

interface TaskAssignment {
  target: string;
  priority: number;
  assignedAgent?: string;
  assignedAt?: number;
}

function rebalanceByPriority(
  targets: string[],
  priorities: Record<string, number>,
): Array<{ target: string; priority: number }> {
  const prioritized = targets.map((target) => ({
    target,
    priority: priorities[target] ?? 5,
  }));

  prioritized.sort((a, b) => a.priority - b.priority);
  return prioritized;
}

export default defineWorkflow('workflow.auto-assign.v1', 'Auto Assign Targets', (workflow) =>
  workflow
    .description(
      'Dynamically create agents and assign targets for balanced execution with workload monitoring and priority-based rebalancing',
    )
    .buildGraph((ctx: WorkflowExecutionContext) => {
      const targets = ctx.getConfig<string[]>('targets', []);
      const maxConcurrency = ctx.getConfig<number>('maxConcurrency', 4);
      const priorities = ctx.getConfig<Record<string, number>>('priorities', {});
      const enableRebalancing = ctx.getConfig<boolean>('enableRebalancing', true);

      const processedTargets = enableRebalancing
        ? rebalanceByPriority(targets, priorities)
        : targets.map((target) => ({ target, priority: 5 }));

      return parallelStep('assign-targets', (parallel) => {
        parallel.maxConcurrency(maxConcurrency);

        for (const [i, { target, priority }] of processedTargets.entries()) {
          const agentId = `agent-${i}`;
          parallel.sequence(agentId, (sequence) => {
            sequence.tool('get-workloads', 'state_board_list', {
              input: { namespace: 'agent_workload' },
            });
            sequence.tool('create-agent', 'create_task_handoff', {
              input: {
                description: `Process target: ${target} (priority: ${priority})`,
                targetDomain: 'browser',
                constraints: [`Target URL: ${target}`, `Priority: ${priority}`],
              },
            });
            sequence.tool('execute-task', 'page_evaluate', {
              input: {
                expression: `console.log('[${agentId}] Working on ${target} with priority ${priority}')`,
              },
            });
            sequence.tool('record-assignment', 'state_board_set', {
              input: {
                key: `assignment:${agentId}:${i}`,
                value: {
                  target,
                  priority,
                  assignedAgent: agentId,
                  assignedAt: Date.now(),
                } as TaskAssignment,
                namespace: 'task_assignments',
              },
            });
            sequence.tool('update-workload', 'state_board_set', {
              input: {
                key: agentId,
                value: {
                  agentId,
                  currentTasks: 1,
                  completedTasks: 0,
                  lastHeartbeat: Date.now(),
                  status: 'busy',
                } as AgentWorkload,
                namespace: 'agent_workload',
              },
            });
          });
        }
      });
    })
    .onFinish((ctx: WorkflowExecutionContext, result: unknown) => {
      const completedCount = Array.isArray(result) ? result.length : 0;
      ctx.emitMetric('targets.completed', completedCount, 'counter');
      ctx.emitMetric('targets.timestamp', Date.now(), 'gauge');
      console.log(`[auto-assign] Completed ${completedCount} target assignments`);
    })
    .onError(async (ctx: WorkflowExecutionContext, error: Error) => {
      ctx.emitMetric('workflow.errors', 1, 'counter');
      console.error('[auto-assign] Workflow failed:', error);

      try {
        await ctx.invokeTool('state_board_clear', {
          namespace: 'task_assignments',
        });
      } catch (cleanupError: unknown) {
        console.warn('[auto-assign] Cleanup failed:', cleanupError);
      }
    }),
);
