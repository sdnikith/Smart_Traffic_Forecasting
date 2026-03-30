#!/usr/bin/env python3
"""
Lineage Tracker for Traffic Platform
"""

import os
import logging
import json
import boto3
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LineageTracker:
    """Tracks data lineage through DBT manifest"""
    
    def __init__(self):
        self.dbt_manifest_path = os.getenv('DBT_MANIFEST_PATH', 'target/manifest.json')
        self.s3_client = boto3.client('s3')
        
    def parse_dbt_manifest(self):
        """Parse DBT manifest.json to extract model dependencies"""
        logger.info("Parsing DBT manifest for lineage")
        
        try:
            with open(self.dbt_manifest_path, 'r') as f:
                manifest = json.load(f)
            
            # Extract model nodes
            models = {}
            for node_id, node in manifest.get('nodes', {}).items():
                if node.get('resource_type') == 'model':
                    models[node_id] = {
                        'name': node.get('name'),
                        'depends_on': node.get('depends_on', {}).get('nodes', []),
                        'sources': node.get('depends_on', {}).get('sources', []),
                        'schema': node.get('schema'),
                        'alias': node.get('alias'),
                        'description': node.get('description'),
                        'config': node.get('config', {})
                    }
            
            # Extract source nodes
            sources = {}
            for node_id, node in manifest.get('sources', {}).items():
                sources[node_id] = {
                    'name': node.get('name'),
                    'schema': node.get('schema'),
                    'identifier': node.get('identifier'),
                    'loader': node.get('loader'),
                    'description': node.get('description')
                }
            
            logger.info(f"Parsed {len(models)} models and {len(sources)} sources from manifest")
            
            return {
                'models': models,
                'sources': sources,
                'manifest_metadata': {
                    'dbt_version': manifest.get('dbt_version'),
                    'generated_at': manifest.get('generated_at'),
                    'project_name': manifest.get('project_name')
                }
            }
            
        except FileNotFoundError:
            logger.error(f"DBT manifest not found at {self.dbt_manifest_path}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing DBT manifest: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error parsing manifest: {e}")
            return None
    
    def build_dependency_graph(self, manifest_data):
        """Build dependency graph from manifest data"""
        logger.info("Building dependency graph")
        
        if not manifest_data:
            return None
        
        models = manifest_data['models']
        sources = manifest_data['sources']
        
        # Build graph structure
        graph = {
            'nodes': [],
            'edges': [],
            'metadata': manifest_data['manifest_metadata']
        }
        
        # Add source nodes
        for source_id, source_info in sources.items():
            graph['nodes'].append({
                'id': source_id,
                'name': source_info['name'],
                'type': 'source',
                'schema': source_info.get('schema'),
                'description': source_info.get('description', '')
            })
        
        # Add model nodes
        for model_id, model_info in models.items():
            graph['nodes'].append({
                'id': model_id,
                'name': model_info['name'],
                'type': 'model',
                'schema': model_info.get('schema'),
                'description': model_info.get('description', ''),
                'materialization': model_info.get('config', {}).get('materialized', 'view')
            })
            
            # Add edges for dependencies
            for dependency in model_info.get('depends_on', {}).get('nodes', []):
                graph['edges'].append({
                    'from': dependency,
                    'to': model_id,
                    'type': 'model_dependency'
                })
            
            # Add edges for source dependencies
            for source_dep in model_info.get('depends_on', {}).get('sources', []):
                graph['edges'].append({
                    'from': source_dep,
                    'to': model_id,
                    'type': 'source_dependency'
                })
        
        logger.info(f"Built graph with {len(graph['nodes'])} nodes and {len(graph['edges'])} edges")
        return graph
    
    def analyze_impact(self, graph, failed_node):
        """Analyze impact of a failed node"""
        logger.info(f"Analyzing impact of failed node: {failed_node}")
        
        # Find all downstream dependencies
        downstream_nodes = set()
        visited = set()
        
        def find_downstream(node_id):
            if node_id in visited:
                return
            
            visited.add(node_id)
            
            for edge in graph['edges']:
                if edge['from'] == node_id:
                    downstream_nodes.add(edge['to'])
                    find_downstream(edge['to'])
        
        find_downstream(failed_node)
        
        # Categorize impact
        impact_analysis = {
            'failed_node': failed_node,
            'direct_downstream': [],
            'indirect_downstream': [],
            'total_affected': len(downstream_nodes),
            'critical_models': [],
            'analytics_models': [],
            'staging_models': []
        }
        
        # Find direct vs indirect downstream
        for edge in graph['edges']:
            if edge['from'] == failed_node:
                impact_analysis['direct_downstream'].append(edge['to'])
        
        # Categorize affected models by schema
        for node_id in downstream_nodes:
            if node_id == failed_node:
                continue
                
            # Find node info
            node_info = next((n for n in graph['nodes'] if n['id'] == node_id), None)
            if node_info:
                if node_info['schema'] == 'ANALYTICS':
                    impact_analysis['analytics_models'].append(node_id)
                elif node_info['schema'] == 'STAGING':
                    impact_analysis['staging_models'].append(node_id)
                
                # Check if it's a critical model (materialized table)
                if node_info.get('materialization') == 'table':
                    impact_analysis['critical_models'].append(node_id)
        
        impact_analysis['indirect_downstream'] = [
            n for n in downstream_nodes 
            if n not in impact_analysis['direct_downstream']
        ]
        
        return impact_analysis
    
    def generate_lineage_report(self, graph):
        """Generate comprehensive lineage report"""
        logger.info("Generating lineage report")
        
        report = {
            'summary': {
                'total_nodes': len(graph['nodes']),
                'total_edges': len(graph['edges']),
                'sources': len([n for n in graph['nodes'] if n['type'] == 'source']),
                'models': len([n for n in graph['nodes'] if n['type'] == 'model']),
                'materialized_tables': len([
                    n for n in graph['nodes'] 
                    if n['type'] == 'model' and n.get('materialization') == 'table'
                ])
            },
            'graph': graph,
            'critical_paths': self.identify_critical_paths(graph),
            'generated_at': datetime.now().isoformat()
        }
        
        return report
    
    def identify_critical_paths(self, graph):
        """Identify critical data paths"""
        logger.info("Identifying critical data paths")
        
        critical_paths = []
        
        # Path from sources to analytics
        source_nodes = [n['id'] for n in graph['nodes'] if n['type'] == 'source']
        analytics_nodes = [n['id'] for n in graph['nodes'] 
                        if n['type'] == 'model' and n.get('schema') == 'ANALYTICS']
        
        for source in source_nodes:
            for analytics in analytics_nodes:
                path = self.find_path(graph, source, analytics)
                if path:
                    critical_paths.append({
                        'from': source,
                        'to': analytics,
                        'path': path,
                        'length': len(path),
                        'critical': any(
                            n.get('materialization') == 'table' 
                            for n in [no for no in graph['nodes'] if no['id'] in path]
                        )
                    })
        
        # Sort by path length and criticality
        critical_paths.sort(key=lambda x: (x['critical'], x['length']), reverse=True)
        
        return critical_paths[:10]  # Top 10 critical paths
    
    def find_path(self, graph, start, end, visited=None):
        """Find path between two nodes using DFS"""
        if visited is None:
            visited = set()
        
        if start == end:
            return [start]
        
        if start in visited:
            return None
        
        visited.add(start)
        
        for edge in graph['edges']:
            if edge['from'] == start:
                path = self.find_path(graph, edge['to'], end, visited.copy())
                if path:
                    return [start] + path
        
        return None
    
    def save_lineage_to_s3(self, report):
        """Save lineage report to S3"""
        logger.info("Saving lineage report to S3")
        
        try:
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            s3_key = f'observability/lineage-report-{timestamp}.json'
            
            self.s3_client.put_object(
                Bucket='traffic-platform-lake',
                Key=s3_key,
                Body=json.dumps(report, indent=2, default=str),
                ContentType='application/json'
            )
            
            logger.info(f"Lineage report saved to S3: {s3_key}")
            return s3_key
            
        except Exception as e:
            logger.error(f"Error saving lineage report to S3: {e}")
            return None
    
    def run_lineage_tracking(self):
        """Run complete lineage tracking"""
        logger.info("Starting lineage tracking")
        
        try:
            # Parse DBT manifest
            manifest_data = self.parse_dbt_manifest()
            
            if not manifest_data:
                logger.error("Failed to parse DBT manifest")
                return None
            
            # Build dependency graph
            graph = self.build_dependency_graph(manifest_data)
            
            if not graph:
                logger.error("Failed to build dependency graph")
                return None
            
            # Generate report
            report = self.generate_lineage_report(graph)
            
            # Save to S3
            s3_key = self.save_lineage_to_s3(report)
            
            # Analyze some example failure scenarios
            if graph['nodes']:
                # Example: What if raw.traffic_events fails?
                raw_traffic_node = next(
                    (n['id'] for n in graph['nodes'] 
                     if n['type'] == 'source' and 'traffic_events' in n['name']), 
                    None
                )
                
                if raw_traffic_node:
                    impact = self.analyze_impact(graph, raw_traffic_node)
                    logger.info(f"Impact if raw.traffic_events fails: {impact['total_affected']} models affected")
                
                # Example: What if stg_traffic_readings fails?
                staging_node = next(
                    (n['id'] for n in graph['nodes'] 
                     if n['type'] == 'model' and 'stg_traffic_readings' in n['name']), 
                    None
                )
                
                if staging_node:
                    impact = self.analyze_impact(graph, staging_node)
                    logger.info(f"Impact if stg_traffic_readings fails: {impact['total_affected']} models affected")
            
            logger.info("Lineage tracking completed successfully")
            return {
                'report': report,
                's3_key': s3_key,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error in lineage tracking: {e}")
            return None

def main():
    """Main lineage tracker function"""
    logger.info("Starting Lineage Tracker")
    
    tracker = LineageTracker()
    
    try:
        # Run lineage tracking
        result = tracker.run_lineage_tracking()
        
        # Log summary
        if result:
            report = result['report']
            logger.info(f"Lineage tracking completed at: {report['generated_at']}")
            logger.info(f"Summary: {report['summary']}")
            if result['s3_key']:
                logger.info(f"Report saved to: {result['s3_key']}")
        
    except Exception as e:
        logger.error(f"Error in lineage tracking: {e}")

if __name__ == "__main__":
    main()
