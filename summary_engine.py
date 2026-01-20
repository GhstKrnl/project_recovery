"""
Heuristic AI Summary Engine
Generates natural language summaries for Overview, Schedule Recovery, Resource Recovery, and Cost Recovery tabs.
"""

import pandas as pd
import numpy as np
from datetime import datetime

def generate_portfolio_summary(df_schedule, df_resource, cost_df_results, resource_stats, rc_df):
    """
    Generates a natural language summary of portfolio status for Overview tab.
    Returns empty string if data is not available.
    """
    if df_schedule is None or df_schedule.empty:
        return ""
    
    summary_parts = []
    
    # Basic portfolio stats
    total_projects = 0
    total_tasks = len(df_schedule)
    
    if "project_id" in df_schedule.columns:
        total_projects = df_schedule["project_id"].nunique()
    
    if "project_name" in df_schedule.columns:
        project_names = df_schedule["project_name"].unique()
        if len(project_names) > 0:
            summary_parts.append(f"The portfolio contains **{total_projects} project(s)** with **{total_tasks} total activities**.")
    
    # Completion status
    if "percent_complete" in df_schedule.columns:
        completed_tasks = df_schedule[df_schedule["percent_complete"] >= 100].shape[0]
        pct_complete = df_schedule["percent_complete"].mean() if not df_schedule["percent_complete"].isna().all() else 0
        
        if pct_complete >= 90:
            summary_parts.append(f"Project completion is **{pct_complete:.1f}%**, with **{completed_tasks} tasks completed**. The portfolio is nearing completion.")
        elif pct_complete >= 50:
            summary_parts.append(f"Project completion stands at **{pct_complete:.1f}%**, with **{completed_tasks} tasks completed**. Progress is on track.")
        else:
            summary_parts.append(f"Project completion is **{pct_complete:.1f}%**, with **{completed_tasks} tasks completed**. Early stage execution.")
    
    # Schedule status
    if "forecast_finish_date" in df_schedule.columns and "planned_finish" in df_schedule.columns:
        forecast_fin = pd.to_datetime(df_schedule["forecast_finish_date"], errors='coerce').max()
        plan_fin = pd.to_datetime(df_schedule["planned_finish"], errors='coerce').max()
        
        if pd.notna(forecast_fin) and pd.notna(plan_fin):
            sched_var = (forecast_fin - plan_fin).days
            if sched_var > 7:
                summary_parts.append(f"⚠️ **Schedule Alert**: The portfolio is forecasted to finish **{sched_var:.0f} days late** (Forecast: {forecast_fin.strftime('%Y-%m-%d')}, Planned: {plan_fin.strftime('%Y-%m-%d')}).")
            elif sched_var > 0:
                summary_parts.append(f"⚠️ **Schedule Concern**: The portfolio is forecasted to finish **{sched_var:.0f} days late**.")
            elif sched_var < -7:
                summary_parts.append(f"✅ **Schedule Ahead**: The portfolio is forecasted to finish **{abs(sched_var):.0f} days early**.")
            else:
                summary_parts.append(f"✅ **Schedule Status**: The portfolio is on schedule.")
    
    # Critical path status
    if "on_critical_path" in df_schedule.columns:
        critical_count = df_schedule["on_critical_path"].sum() if df_schedule["on_critical_path"].dtype == bool else 0
        if critical_count > 0:
            summary_parts.append(f"**{critical_count} activities** are on the critical path, requiring close monitoring.")
    
    # Cost status
    if not cost_df_results.empty and "eac_cost" in cost_df_results.columns and "planned_cost" in cost_df_results.columns:
        total_planned = cost_df_results["planned_cost"].sum()
        total_eac = cost_df_results["eac_cost"].sum()
        if total_planned > 0:
            cost_var = total_eac - total_planned
            cost_var_pct = (cost_var / total_planned) * 100
            if cost_var > 1000:
                summary_parts.append(f"⚠️ **Cost Alert**: Estimated cost overrun of **${cost_var:,.0f}** ({cost_var_pct:.1f}%) above planned budget.")
            elif cost_var < -1000:
                summary_parts.append(f"✅ **Cost Performance**: Estimated savings of **${abs(cost_var):,.0f}** ({abs(cost_var_pct):.1f}%) below planned budget.")
            else:
                summary_parts.append(f"✅ **Cost Status**: Estimated costs are aligned with planned budget.")
    
    # Resource status
    if resource_stats:
        overloaded_resources = sum(1 for stats in resource_stats.values() if stats.get("overload_days_count", 0) > 0)
        if overloaded_resources > 0:
            summary_parts.append(f"⚠️ **Resource Alert**: **{overloaded_resources} resource(s)** are currently overallocated and may require attention.")
        else:
            summary_parts.append(f"✅ **Resource Status**: All resources are properly allocated.")
    
    # Risk status
    if rc_df is not None and not rc_df.empty:
        risk_count = len(rc_df)
        if risk_count > 0:
            summary_parts.append(f"⚠️ **Risk Alert**: **{risk_count} root cause(s)** have been identified requiring mitigation.")
        else:
            summary_parts.append(f"✅ **Risk Status**: No significant root causes detected.")
    
    if summary_parts:
        return " ".join(summary_parts)
    else:
        return "Portfolio analysis is in progress. Please ensure all data is loaded and analysis has been run."


def generate_schedule_summary(df_schedule, rc_df):
    """
    Generates a natural language summary of schedule recovery status for Schedule Recovery tab.
    Returns empty string if data is not available.
    """
    if df_schedule is None or df_schedule.empty:
        return ""
    
    summary_parts = []
    
    # Project-level analysis
    if "project_name" in df_schedule.columns:
        projects = df_schedule["project_name"].unique()
        summary_parts.append(f"**Schedule Analysis** covers **{len(projects)} project(s)**: {', '.join(projects[:5])}{'...' if len(projects) > 5 else ''}.")
    
    # Delayed activities
    if "total_schedule_delay" in df_schedule.columns:
        delayed_activities = df_schedule[df_schedule["total_schedule_delay"] > 0]
        if len(delayed_activities) > 0:
            max_delay = delayed_activities["total_schedule_delay"].max()
            summary_parts.append(f"**{len(delayed_activities)} activities** are experiencing delays, with the maximum delay being **{max_delay:.1f} days**.")
            
            # Top delayed activities by task_created_delay
            if "task_created_delay" in delayed_activities.columns:
                top_delayed = delayed_activities.nlargest(3, "task_created_delay")
                if len(top_delayed) > 0:
                    delay_items = []
                    for _, row in top_delayed.iterrows():
                        proj_name = row.get("project_name", "Unknown Project")
                        act_id = row.get("activity_id", "Unknown")
                        act_name = row.get("activity_name", "")
                        delay = row.get("task_created_delay", 0)
                        if delay > 0:
                            name_str = f"{act_name}" if act_name and str(act_name) != "nan" else f"Activity {act_id}"
                            delay_items.append(f"**{proj_name} - {name_str}** ({delay:.1f}d delay)")
                    if delay_items:
                        summary_parts.append(f"Top delayed activities: {', '.join(delay_items)}.")
        else:
            summary_parts.append("✅ **All activities are on schedule** with no delays detected.")
    
    # Critical path status
    if "on_critical_path" in df_schedule.columns:
        critical_count = df_schedule["on_critical_path"].sum() if df_schedule["on_critical_path"].dtype == bool else 0
        if critical_count > 0:
            summary_parts.append(f"**{critical_count} activities** are on the critical path and require immediate attention to prevent further delays.")
    
    # Root causes
    if rc_df is not None and not rc_df.empty and "Root Cause Category" in rc_df.columns:
        rc_by_category = rc_df["Root Cause Category"].value_counts()
        if len(rc_by_category) > 0:
            top_category = rc_by_category.index[0]
            count = rc_by_category.iloc[0]
            summary_parts.append(f"**Primary root cause**: **{top_category}** affecting **{count} activity(s)**.")
            
            # Activities with this root cause
            if "activity_id" in rc_df.columns and "project_name" in rc_df.columns:
                top_rc_activities = rc_df[rc_df["Root Cause Category"] == top_category].head(3)
                if len(top_rc_activities) > 0:
                    rc_items = []
                    for _, row in top_rc_activities.iterrows():
                        proj_name = row.get("project_name", "Unknown Project")
                        act_id = row.get("activity_id", "Unknown")
                        rc_items.append(f"**{proj_name} - Activity {act_id}**")
                    if rc_items:
                        summary_parts.append(f"Affected activities: {', '.join(rc_items)}.")
    
    if summary_parts:
        return " ".join(summary_parts)
    else:
        return "Schedule analysis is in progress. Please ensure analysis has been run."


def generate_resource_summary(df_schedule, df_resource, resource_stats, generated_actions):
    """
    Generates a natural language summary of resource management status for Resource Recovery tab.
    Returns empty string if data is not available.
    """
    if df_schedule is None or df_schedule.empty:
        return ""
    
    summary_parts = []
    
    # Resource overview
    if df_resource is not None and not df_resource.empty:
        total_resources = len(df_resource)
        summary_parts.append(f"**Resource Management** covers **{total_resources} resource(s)** across the portfolio.")
    
    # Overloaded resources
    if resource_stats:
        overloaded_resources = []
        for res_id, stats in resource_stats.items():
            overload_days = stats.get("overload_days_count", 0)
            if overload_days > 0:
                overloaded_resources.append((res_id, overload_days))
        
        if overloaded_resources:
            overloaded_resources.sort(key=lambda x: x[1], reverse=True)
            top_overloaded = overloaded_resources[:3]
            overload_items = []
            for res_id, days in top_overloaded:
                # Try to get resource name
                res_name = str(res_id)
                if df_resource is not None and "resource_id" in df_resource.columns:
                    res_row = df_resource[df_resource["resource_id"].astype(str) == str(res_id)]
                    if not res_row.empty and "resource_name" in res_row.columns:
                        res_name = res_row.iloc[0].get("resource_name", str(res_id))
                overload_items.append(f"**{res_name}** ({days:.0f}d overload)")
            
            summary_parts.append(f"⚠️ **{len(overloaded_resources)} resource(s)** are overallocated: {', '.join(overload_items)}.")
        else:
            summary_parts.append("✅ **All resources are properly allocated** with no overloads detected.")
    
    # Recovery actions available
    if generated_actions:
        resource_actions = [a for a in generated_actions if a.get('type') in ['RES_SWAP', 'FTE_ADJ']]
        if resource_actions:
            swap_count = sum(1 for a in resource_actions if a.get('type') == 'RES_SWAP')
            fte_count = sum(1 for a in resource_actions if a.get('type') == 'FTE_ADJ')
            
            action_items = []
            if swap_count > 0:
                action_items.append(f"**{swap_count} resource swap(s)**")
            if fte_count > 0:
                action_items.append(f"**{fte_count} FTE adjustment(s)**")
            
            if action_items:
                summary_parts.append(f"**Recovery opportunities available**: {', '.join(action_items)} to optimize resource allocation.")
    
    # Project references
    if "project_name" in df_schedule.columns:
        projects = df_schedule["project_name"].unique()
        if len(projects) > 0:
            summary_parts.append(f"Resources are allocated across **{len(projects)} project(s)**: {', '.join(projects[:3])}{'...' if len(projects) > 3 else ''}.")
    
    if summary_parts:
        return " ".join(summary_parts)
    else:
        return "Resource analysis is in progress. Please ensure analysis has been run."


def generate_cost_summary(df_schedule, cost_df_results):
    """
    Generates a natural language summary of cost management status for Cost Recovery tab.
    Returns empty string if data is not available.
    """
    if df_schedule is None or df_schedule.empty or cost_df_results.empty:
        return ""
    
    summary_parts = []
    
    # Project references
    if "project_name" in df_schedule.columns:
        projects = df_schedule["project_name"].unique()
        summary_parts.append(f"**Cost Analysis** covers **{len(projects)} project(s)**: {', '.join(projects[:5])}{'...' if len(projects) > 5 else ''}.")
    
    # Cost performance
    if "planned_cost" in cost_df_results.columns and "eac_cost" in cost_df_results.columns:
        total_planned = cost_df_results["planned_cost"].sum()
        total_eac = cost_df_results["eac_cost"].sum()
        
        if total_planned > 0:
            cost_var = total_eac - total_planned
            cost_var_pct = (cost_var / total_planned) * 100
            
            if cost_var > 10000:
                summary_parts.append(f"⚠️ **Significant Cost Overrun**: Estimated cost exceeds planned budget by **${cost_var:,.0f}** ({cost_var_pct:.1f}%).")
            elif cost_var > 1000:
                summary_parts.append(f"⚠️ **Cost Overrun**: Estimated cost exceeds planned budget by **${cost_var:,.0f}** ({cost_var_pct:.1f}%).")
            elif cost_var < -10000:
                summary_parts.append(f"✅ **Significant Cost Savings**: Estimated cost is **${abs(cost_var):,.0f}** ({abs(cost_var_pct):.1f}%) below planned budget.")
            elif cost_var < -1000:
                summary_parts.append(f"✅ **Cost Savings**: Estimated cost is **${abs(cost_var):,.0f}** ({abs(cost_var_pct):.1f}%) below planned budget.")
            else:
                summary_parts.append(f"✅ **Cost Performance**: Estimated costs are aligned with planned budget (${total_eac:,.0f} vs ${total_planned:,.0f}).")
    
    # Top cost overruns
    if "cost_variance" in cost_df_results.columns or ("actual_cost" in cost_df_results.columns and "planned_cost" in cost_df_results.columns):
        if "cost_variance" not in cost_df_results.columns:
            cost_df_results = cost_df_results.copy()
            cost_df_results["cost_variance"] = cost_df_results.get("actual_cost", 0) - cost_df_results.get("planned_cost", 0)
        
        overruns = cost_df_results[cost_df_results["cost_variance"] > 1000]
        if len(overruns) > 0:
            top_overruns = overruns.nlargest(3, "cost_variance")
            overrun_items = []
            for _, row in top_overruns.iterrows():
                act_id = row.get("activity_id", "Unknown")
                var = row.get("cost_variance", 0)
                proj_name = "Unknown"
                if "activity_id" in df_schedule.columns:
                    act_row = df_schedule[df_schedule["activity_id"].astype(str) == str(act_id)]
                    if not act_row.empty and "project_name" in act_row.columns:
                        proj_name = act_row.iloc[0].get("project_name", "Unknown")
                overrun_items.append(f"**{proj_name} - Activity {act_id}** (${var:,.0f} overrun)")
            
            if overrun_items:
                summary_parts.append(f"Top cost overruns: {', '.join(overrun_items)}.")
    
    # Cost breakdown
    if "actual_cost" in cost_df_results.columns and "remaining_cost" in cost_df_results.columns:
        total_actual = cost_df_results["actual_cost"].sum()
        total_remaining = cost_df_results["remaining_cost"].sum()
        if total_actual > 0 or total_remaining > 0:
            summary_parts.append(f"Current spending: **${total_actual:,.0f}** actual, **${total_remaining:,.0f}** remaining to complete.")
    
    if summary_parts:
        return " ".join(summary_parts)
    else:
        return "Cost analysis is in progress. Please ensure analysis has been run."
