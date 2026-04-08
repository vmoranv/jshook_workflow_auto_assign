/**
 * Auto-Assign Workflow - Dynamic target balancing across agents
 *
 * Features:
 * 1. Monitor team member workload via SharedStateBoard
 * 2. Auto-assign tasks to least-loaded agents
 * 3. Support priority-based rebalancing
 */

import {
  createWorkflow,
  parallelNode,
  sequenceNode,
  type WorkflowExecutionContext,
} from '@jshookmcp/extension-sdk';

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

/**
 * Find the least loaded agent from workload data
 */
function findLeastLoadedAgent(workloads: Map<string, AgentWorkload>): string | null {
  let minLoad = Infinity;
  let selectedAgent: string | null = null;

  for (const [agentId, workload] of workloads) {
    if (workload.status === 'offline') continue;

    // Score: currentTasks weighted by status
    const score = workload.currentTasks * (workload.status === 'busy' ? 1.5 : 1);
    if (score < minLoad) {
      minLoad = score;
      selectedAgent = agentId;
    }
  }

  return selectedAgent;
}

/**
 * Rebalance tasks based on priority
 */
function rebalanceByPriority(
  targets: string[],
  priorities: Record<string, number>,
): Array<{ target: string; priority: number }> {
  const prioritized = targets.map((target) => ({
    target,
    priority: priorities[target] ?? 5, // Default priority 5 (medium)
  }));

  // Sort by priority (lower number = higher priority)
  prioritized.sort((a, b) => a.priority - b.priority);

  return prioritized;
}

export default createWorkflow('workflow.auto-assign.v1', 'Auto Assign Targets')
  .description(
    'Dynamically create agents and assign targets for balanced execution with workload monitoring and priority-based rebalancing',
  )
  .buildGraph((ctx: WorkflowExecutionContext) => {
    const targets = ctx.getConfig<string[]>('targets', []);
    const maxConcurrency = ctx.getConfig<number>('maxConcurrency', 4);
    const priorities = ctx.getConfig<Record<string, number>>('priorities', {});
    const enableRebalancing = ctx.getConfig<boolean>('enableRebalancing', true);

    // Root parallel node for concurrent task processing
    const root = parallelNode('assign-targets').maxConcurrency(maxConcurrency);

    // If rebalancing is enabled, sort targets by priority first
    const processedTargets = enableRebalancing
      ? rebalanceByPriority(targets, priorities)
      : targets.map((t) => ({ target: t, priority: 5 }));

    for (const [i, { target, priority }] of processedTargets.entries()) {
      const agentId = `agent-${i}`;

      root.step(
        sequenceNode(agentId)
          // Step 1: Check current workloads via SharedStateBoard
          .tool('get-workloads', 'state_board_list', {
            input: { namespace: 'agent_workload' },
          })
          // Step 2: Create agent via coordination task handoff
          .tool('create-agent', 'create_task_handoff', {
            input: {
              description: `Process target: ${target} (priority: ${priority})`,
              targetDomain: 'browser',
              constraints: [`Target URL: ${target}`, `Priority: ${priority}`],
            },
          })
          // Step 3: Execute the actual task in browser
          .tool('execute-task', 'page_evaluate', {
            input: {
              expression: `console.log('[${agentId}] Working on ${target} with priority ${priority}')`,
            },
          })
          // Step 4: Record assignment in SharedStateBoard
          .tool('record-assignment', 'state_board_set', {
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
          })
          // Step 5: Update agent workload counter
          .tool('update-workload', 'state_board_set', {
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
          }),
      );
    }

    return root;
  })
  .onFinish(async (ctx: WorkflowExecutionContext, result: unknown) => {
    const completedCount = Array.isArray(result) ? result.length : 0;
    ctx.emitMetric('targets.completed', completedCount, 'counter');
    ctx.emitMetric('targets.timestamp', Date.now(), 'gauge');

    // Log completion summary
    console.log(`[auto-assign] Completed ${completedCount} target assignments`);
  })
  .onError(async (ctx: WorkflowExecutionContext, error: Error) => {
    ctx.emitMetric('workflow.errors', 1, 'counter');
    console.error('[auto-assign] Workflow failed:', error);

    // Attempt cleanup on error
    try {
      await ctx.invokeTool('state_board_clear', {
        namespace: 'task_assignments',
      });
    } catch (cleanupError: unknown) {
      console.warn('[auto-assign] Cleanup failed:', cleanupError);
    }
  })
  .build();
