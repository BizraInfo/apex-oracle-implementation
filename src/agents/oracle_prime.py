"""
Oracle Prime Agent

This module implements the Oracle Prime agent, which serves as the Meta-Cognitive Orchestrator
for the Apex Oracle 3.0 system. Oracle Prime coordinates the distributed intelligence across
all agents and modules, manages strategic decision-making, and optimizes resource allocation.

Key capabilities:
- Distributed Intelligence Coordination
- Strategic Decision Architecture
- Multi-objective Optimization
- System-wide Resource Allocation

Technologies:
- Multi-agent systems coordination
- Hierarchical planning
- Decision theory
- Resource optimization algorithms
"""

import os
import logging
import uuid
import json
import time
from typing import Dict, List, Any, Optional, Union, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

class OraclePrime:
    """
    Oracle Prime Agent - Meta-Cognitive Orchestrator
    
    This agent serves as the central coordination point for the Apex Oracle 3.0 system,
    managing agent interactions, resource allocation, and strategic decision-making.
    """
    
    def __init__(self, config: Dict[str, Any], knowledge_graph=None):
        """
        Initialize the Oracle Prime agent.
        
        Args:
            config: Configuration dictionary with agent parameters
            knowledge_graph: Reference to the knowledge graph interface
        """
        self.agent_id = f"oracle_prime_{uuid.uuid4().hex[:8]}"
        self.logger = logging.getLogger(f"apex.agents.{self.agent_id}")
        self.config = config
        self.knowledge_graph = knowledge_graph
        
        # Agent registry
        self.registered_agents = {}
        
        # Task management
        self.active_tasks = {}
        self.task_history = []
        
        # Resource allocation
        self.resource_pool = {}
        self.allocation_strategy = self.config.get("allocation_strategy", "priority_based")
        
        # Performance metrics
        self.metrics = {
            "tasks_completed": 0,
            "tasks_failed": 0,
            "average_completion_time": 0,
            "resource_utilization": {},
        }
        
        self.logger.info(f"Oracle Prime agent {self.agent_id} initialized")
        self._register_with_knowledge_graph()
    
    def _register_with_knowledge_graph(self):
        """Register agent information with the knowledge graph."""
        if self.knowledge_graph:
            try:
                entity_id = self.knowledge_graph.create_entity(
                    name=f"Oracle Prime ({self.agent_id})",
                    entity_type="Agent",
                    properties={
                        "role": "Meta-Cognitive Orchestrator",
                        "status": "active",
                        "initialization_time": datetime.now().isoformat()
                    },
                    observations=[
                        "Distributed Intelligence Coordination",
                        "Strategic Decision Architecture",
                        "Multi-objective Optimization",
                        "System-wide Resource Allocation"
                    ]
                )
                self.kg_entity_id = entity_id
                self.logger.info(f"Registered with knowledge graph as entity {entity_id}")
            except Exception as e:
                self.logger.error(f"Failed to register with knowledge graph: {str(e)}")
    
    def register_agent(self, agent_id: str, agent_type: str, capabilities: List[str], 
                      endpoint: Optional[str] = None) -> bool:
        """
        Register an agent with Oracle Prime.
        
        Args:
            agent_id: Unique identifier for the agent
            agent_type: Type/role of the agent
            capabilities: List of agent capabilities
            endpoint: Optional communication endpoint
            
        Returns:
            Success status of registration
        """
        if agent_id in self.registered_agents:
            self.logger.warning(f"Agent {agent_id} already registered")
            return False
        
        self.registered_agents[agent_id] = {
            "agent_type": agent_type,
            "capabilities": capabilities,
            "endpoint": endpoint,
            "status": "active",
            "registration_time": datetime.now().isoformat(),
            "last_activity": datetime.now().isoformat(),
            "assigned_tasks": [],
            "performance_metrics": {
                "tasks_completed": 0,
                "success_rate": 1.0,
                "average_response_time": 0
            }
        }
        
        self.logger.info(f"Agent {agent_id} of type {agent_type} registered successfully")
        
        # Register the relationship in the knowledge graph
        if self.knowledge_graph and hasattr(self, 'kg_entity_id'):
            try:
                # Create entity for the agent if it doesn't exist
                agent_entity_id = self.knowledge_graph.create_entity(
                    name=f"{agent_type} ({agent_id})",
                    entity_type="Agent",
                    properties={
                        "status": "active",
                        "registration_time": datetime.now().isoformat()
                    },
                    observations=capabilities
                )
                
                # Create relationship
                self.knowledge_graph.create_relation(
                    from_entity_id=self.kg_entity_id,
                    relation_type="COORDINATES",
                    to_entity_id=agent_entity_id,
                    properties={
                        "registration_time": datetime.now().isoformat()
                    }
                )
                
                # Store the knowledge graph entity ID
                self.registered_agents[agent_id]["kg_entity_id"] = agent_entity_id
                
            except Exception as e:
                self.logger.error(f"Failed to register agent relationship in knowledge graph: {str(e)}")
        
        return True
    
    def allocate_task(self, task_spec: Dict[str, Any]) -> Dict[str, Any]:
        """
        Allocate a task to appropriate agent(s) based on capabilities and load.
        
        Args:
            task_spec: Task specification including requirements and constraints
            
        Returns:
            Task allocation details including assigned agents and expected completion
        """
        task_id = task_spec.get("task_id", f"task_{uuid.uuid4().hex[:8]}")
        task_type = task_spec.get("task_type", "unknown")
        required_capabilities = task_spec.get("required_capabilities", [])
        priority = task_spec.get("priority", 5)  # 1-10 scale
        deadline = task_spec.get("deadline")
        
        self.logger.info(f"Allocating task {task_id} of type {task_type} with priority {priority}")
        
        # Find suitable agents based on capabilities
        suitable_agents = []
        for agent_id, agent_info in self.registered_agents.items():
            if agent_info["status"] != "active":
                continue
                
            # Check if agent has all required capabilities
            if all(cap in agent_info["capabilities"] for cap in required_capabilities):
                # Calculate agent load factor (0-1 scale)
                load_factor = len(agent_info["assigned_tasks"]) / 10  # Assuming max 10 tasks per agent
                
                # Calculate suitability score
                capability_match = sum(1 for cap in required_capabilities if cap in agent_info["capabilities"])
                capability_score = capability_match / len(required_capabilities) if required_capabilities else 1.0
                
                # Performance factor
                performance_factor = agent_info["performance_metrics"]["success_rate"]
                
                # Combined score (higher is better)
                suitability_score = (capability_score * 0.5 + performance_factor * 0.3) * (1 - load_factor * 0.8)
                
                suitable_agents.append({
                    "agent_id": agent_id,
                    "score": suitability_score,
                    "load_factor": load_factor
                })
        
        if not suitable_agents:
            self.logger.warning(f"No suitable agents found for task {task_id}")
            return {
                "task_id": task_id,
                "status": "unallocated",
                "reason": "no_suitable_agents",
                "timestamp": datetime.now().isoformat()
            }
        
        # Sort by suitability score (higher first)
        suitable_agents.sort(key=lambda x: x["score"], reverse=True)
        
        # Determine allocation strategy based on task requirements
        if task_spec.get("parallel_execution", False) and len(suitable_agents) > 1:
            # Parallel allocation to multiple agents
            num_agents = min(task_spec.get("parallelism", 2), len(suitable_agents))
            selected_agents = suitable_agents[:num_agents]
            allocation_type = "parallel"
        else:
            # Allocate to the most suitable agent
            selected_agents = [suitable_agents[0]]
            allocation_type = "single"
        
        # Create task allocation
        allocation = {
            "task_id": task_id,
            "allocation_type": allocation_type,
            "allocated_agents": [a["agent_id"] for a in selected_agents],
            "allocation_time": datetime.now().isoformat(),
            "expected_completion": None,  # To be calculated
            "priority": priority,
            "status": "allocated"
        }
        
        # Update agent task assignments
        for agent in selected_agents:
            agent_id = agent["agent_id"]
            self.registered_agents[agent_id]["assigned_tasks"].append(task_id)
            self.registered_agents[agent_id]["last_activity"] = datetime.now().isoformat()
        
        # Store the active task
        self.active_tasks[task_id] = {
            "spec": task_spec,
            "allocation": allocation,
            "start_time": datetime.now().isoformat(),
            "status": "allocated",
            "progress": 0.0
        }
        
        # Log allocation
        agent_names = ", ".join([a["agent_id"] for a in selected_agents])
        self.logger.info(f"Task {task_id} allocated to {agent_names} with strategy {allocation_type}")
        
        # Record in knowledge graph if available
        if self.knowledge_graph and hasattr(self, 'kg_entity_id'):
            try:
                # Create task entity
                task_entity_id = self.knowledge_graph.create_entity(
                    name=f"Task {task_id}",
                    entity_type="Task",
                    properties={
                        "task_type": task_type,
                        "priority": priority,
                        "allocation_type": allocation_type,
                        "status": "allocated"
                    },
                    observations=[
                        f"Task {task_id} of type {task_type}",
                        f"Required capabilities: {', '.join(required_capabilities)}",
                        f"Priority level: {priority}/10"
                    ]
                )
                
                # Create relationships between task and agents
                for agent_id in allocation["allocated_agents"]:
                    if "kg_entity_id" in self.registered_agents[agent_id]:
                        self.knowledge_graph.create_relation(
                            from_entity_id=self.registered_agents[agent_id]["kg_entity_id"],
                            relation_type="ASSIGNED_TO",
                            to_entity_id=task_entity_id
                        )
                
                # Store the knowledge graph entity ID
                self.active_tasks[task_id]["kg_entity_id"] = task_entity_id
                
            except Exception as e:
                self.logger.error(f"Failed to record task allocation in knowledge graph: {str(e)}")
        
        return allocation
    
    def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """
        Get the current status of a task.
        
        Args:
            task_id: ID of the task to check
            
        Returns:
            Task status information
        """
        if task_id not in self.active_tasks and not any(t["task_id"] == task_id for t in self.task_history):
            return {"task_id": task_id, "status": "unknown", "error": "Task not found"}
        
        if task_id in self.active_tasks:
            return {
                "task_id": task_id,
                "status": self.active_tasks[task_id]["status"],
                "progress": self.active_tasks[task_id]["progress"],
                "allocation": self.active_tasks[task_id]["allocation"],
                "start_time": self.active_tasks[task_id]["start_time"]
            }
        else:
            # Find in history
            for task in self.task_history:
                if task["task_id"] == task_id:
                    return task
    
    def update_task_status(self, task_id: str, status: str, progress: float = None, 
                          result: Any = None) -> bool:
        """
        Update the status of a task.
        
        Args:
            task_id: ID of the task to update
            status: New status (in_progress, completed, failed)
            progress: Optional progress percentage (0-100)
            result: Optional task result data
            
        Returns:
            Success status of the update
        """
        if task_id not in self.active_tasks:
            self.logger.warning(f"Cannot update unknown task {task_id}")
            return False
        
        task = self.active_tasks[task_id]
        old_status = task["status"]
        task["status"] = status
        
        if progress is not None:
            task["progress"] = progress
        
        if result is not None:
            task["result"] = result
        
        task["last_updated"] = datetime.now().isoformat()
        
        # Handle task completion or failure
        if status in ["completed", "failed"]:
            # Calculate task duration
            start_time = datetime.fromisoformat(task["start_time"])
            end_time = datetime.now()
            duration_seconds = (end_time - start_time).total_seconds()
            
            # Update task record
            task["end_time"] = end_time.isoformat()
            task["duration_seconds"] = duration_seconds
            
            # Update agent metrics
            for agent_id in task["allocation"]["allocated_agents"]:
                agent = self.registered_agents[agent_id]
                
                # Remove from active tasks
                if task_id in agent["assigned_tasks"]:
                    agent["assigned_tasks"].remove(task_id)
                
                # Update performance metrics
                agent["performance_metrics"]["tasks_completed"] += 1
                
                # Update success rate
                completed = agent["performance_metrics"]["tasks_completed"]
                success_rate = agent["performance_metrics"]["success_rate"]
                if status == "completed":
                    # Weighted update (more weight to recent tasks)
                    agent["performance_metrics"]["success_rate"] = (success_rate * (completed - 1) + 1.0) / completed
                else:  # failed
                    agent["performance_metrics"]["success_rate"] = (success_rate * (completed - 1) + 0.0) / completed
            
            # Move to history
            self.task_history.append(task.copy())
            del self.active_tasks[task_id]
            
            # Update system metrics
            if status == "completed":
                self.metrics["tasks_completed"] += 1
                # Update average completion time
                completed = self.metrics["tasks_completed"]
                avg_time = self.metrics["average_completion_time"]
                self.metrics["average_completion_time"] = (avg_time * (completed - 1) + duration_seconds) / completed
            else:  # failed
                self.metrics["tasks_failed"] += 1
        
        # Log status change
        self.logger.info(f"Task {task_id} status updated: {old_status} -> {status}")
        
        # Update knowledge graph if available
        if self.knowledge_graph and "kg_entity_id" in task:
            try:
                # Add observation about status change
                self.knowledge_graph.add_observation(
                    entity_id=task["kg_entity_id"],
                    observation=f"Status changed from {old_status} to {status} at {datetime.now().isoformat()}"
                )
                
                if status in ["completed", "failed"]:
                    # Update task properties
                    self.knowledge_graph.update_entity_properties(
                        entity_id=task["kg_entity_id"],
                        properties={
                            "status": status,
                            "duration_seconds": duration_seconds,
                            "end_time": end_time.isoformat()
                        }
                    )
            except Exception as e:
                self.logger.error(f"Failed to update task status in knowledge graph: {str(e)}")
        
        return True
    
    def get_system_status(self) -> Dict[str, Any]:
        """
        Get the current status of the overall system.
        
        Returns:
            System status information including agents, tasks, and metrics
        """
        active_agents = sum(1 for a in self.registered_agents.values() if a["status"] == "active")
        
        return {
            "agent_id": self.agent_id,
            "timestamp": datetime.now().isoformat(),
            "agents": {
                "total": len(self.registered_agents),
                "active": active_agents,
                "inactive": len(self.registered_agents) - active_agents
            },
            "tasks": {
                "active": len(self.active_tasks),
                "completed": self.metrics["tasks_completed"],
                "failed": self.metrics["tasks_failed"],
                "success_rate": self.metrics["tasks_completed"] / 
                    (self.metrics["tasks_completed"] + self.metrics["tasks_failed"]) 
                    if (self.metrics["tasks_completed"] + self.metrics["tasks_failed"]) > 0 else 1.0
            },
            "performance": {
                "average_task_completion_time": self.metrics["average_completion_time"],
                "resource_utilization": self.metrics["resource_utilization"]
            }
        }
    
    def execute_strategic_decision(self, decision_context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a strategic decision based on the provided context.
        
        This is a high-level orchestration function that coordinates complex
        decision-making across multiple agents and components.
        
        Args:
            decision_context: Context information for the decision
            
        Returns:
            Decision outcome and execution details
        """
        decision_id = decision_context.get("decision_id", f"decision_{uuid.uuid4().hex[:8]}")
        decision_type = decision_context.get("decision_type", "standard")
        objectives = decision_context.get("objectives", [])
        constraints = decision_context.get("constraints", [])
        
        self.logger.info(f"Executing strategic decision {decision_id} of type {decision_type}")
        
        # Step 1: Knowledge gathering and situation assessment
        situation_assessment = self._assess_situation(decision_context)
        
        # Step 2: Generate and evaluate alternatives
        alternatives = self._generate_alternatives(decision_context, situation_assessment)
        evaluation = self._evaluate_alternatives(alternatives, objectives, constraints)
        
        # Step 3: Select optimal course of action
        selected_action = self._select_action(evaluation)
        
        # Step 4: Execution planning
        execution_plan = self._create_execution_plan(selected_action)
        
        # Step 5: Allocate tasks for execution
        execution_tasks = []
        for task_spec in execution_plan["tasks"]:
            allocation = self.allocate_task(task_spec)
            execution_tasks.append({
                "task_spec": task_spec,
                "allocation": allocation
            })
        
        # Step 6: Monitor execution (non-blocking)
        self._monitor_execution(execution_tasks)
        
        # Return initial decision outcome
        return {
            "decision_id": decision_id,
            "status": "in_progress",
            "selected_action": selected_action,
            "execution_plan": execution_plan,
            "execution_tasks": execution_tasks,
            "start_time": datetime.now().isoformat()
        }
    
    def _assess_situation(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Internal method for situation assessment."""
        # This would be implemented with actual assessment logic
        return {
            "situation_type": "standard",
            "complexity": "medium",
            "uncertainty": "low",
            "time_pressure": "medium",
            "key_factors": ["factor1", "factor2"],
            "timestamp": datetime.now().isoformat()
        }
    
    def _generate_alternatives(self, context: Dict[str, Any], 
                              assessment: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Internal method to generate decision alternatives."""
        # This would be implemented with actual alternative generation logic
        return [
            {"id": "alt1", "description": "Alternative 1", "approach": "conservative"},
            {"id": "alt2", "description": "Alternative 2", "approach": "balanced"},
            {"id": "alt3", "description": "Alternative 3", "approach": "aggressive"}
        ]
    
    def _evaluate_alternatives(self, alternatives: List[Dict[str, Any]], 
                              objectives: List[Dict[str, Any]],
                              constraints: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Internal method to evaluate alternatives against objectives and constraints."""
        # This would be implemented with actual evaluation logic
        evaluations = {}
        for alt in alternatives:
            evaluations[alt["id"]] = {
                "scores": {"objective1": 0.8, "objective2": 0.6},
                "constraint_violations": 0,
                "overall_score": 0.7
            }
        return evaluations
    
    def _select_action(self, evaluation: Dict[str, Any]) -> Dict[str, Any]:
        """Internal method to select the optimal course of action."""
        # This would select the highest scoring alternative
        best_alt_id = max(evaluation.keys(), key=lambda k: evaluation[k]["overall_score"])
        return {
            "selected_id": best_alt_id,
            "rationale": "Highest overall score",
            "confidence": 0.85,
            "selection_time": datetime.now().isoformat()
        }
    
    def _create_execution_plan(self, action: Dict[str, Any]) -> Dict[str, Any]:
        """Internal method to create an execution plan for the selected action."""
        # This would be implemented with actual planning logic
        return {
            "action_id": action["selected_id"],
            "phases": ["preparation", "execution", "verification"],
            "tasks": [
                {
                    "task_id": f"task_{uuid.uuid4().hex[:8]}",
                    "task_type": "preparation",
                    "required_capabilities": ["data_analysis"],
                    "priority": 7,
                    "description": "Prepare data for execution"
                },
                {
                    "task_id": f"task_{uuid.uuid4().hex[:8]}",
                    "task_type": "execution",
                    "required_capabilities": ["process_execution"],
                    "priority": 8,
                    "description": "Execute core process"
                },
                {
                    "task_id": f"task_{uuid.uuid4().hex[:8]}",
                    "task_type": "verification",
                    "required_capabilities": ["quality_assurance"],
                    "priority": 6,
                    "description": "Verify execution results"
                }
            ],
            "dependencies": [
                {"from": 0, "to": 1, "type": "finish_to_start"},
                {"from": 1, "to": 2, "type": "finish_to_start"}
            ],
            "estimated_duration": 3600  # seconds
        }
    
    def _monitor_execution(self, execution_tasks: List[Dict[str, Any]]):
        """Internal method to monitor execution of tasks (non-blocking)."""
        # This would be implemented with actual monitoring logic
        # For example, spawning a background thread to check status periodically
        def monitor_thread():
            # This is a simplified monitoring routine
            time.sleep(10)  # Just for demonstration
            
            for task in execution_tasks:
                task_id = task["task_spec"]["task_id"]
                if task_id in self.active_tasks:
                    # Simulate progress updates
                    self.update_task_status(task_id, "in_progress", progress=50.0)
                    time.sleep(5)  # Just for demonstration
                    self.update_task_status(task_id, "completed", progress=100.0, 
                                           result={"outcome": "success"})
        
        # Start monitoring in background
        with ThreadPoolExecutor(max_workers=1) as executor:
            executor.submit(monitor_thread)
