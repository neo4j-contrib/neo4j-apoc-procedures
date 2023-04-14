/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.algo;

import apoc.result.VirtualNode;
import apoc.result.VirtualPath;
import apoc.result.VirtualRelationship;
import apoc.util.Util;
import org.neo4j.graphalgo.EstimateEvaluator;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.neo4j.graphdb.RelationshipType.withName;

public class TravellingSalesman {
    
    public static class Tour {
        private final List<Node> travel;
        private final EstimateEvaluator<Double> evaluator;

        public Tour(List<Node> travel, EstimateEvaluator<Double> evaluator) {
            this.travel = new ArrayList<>(travel);
            this.evaluator = evaluator;
        }

        public void swapCities() {
            int a = generateRandomIndex();
            int b = generateRandomIndex();
            while(a == b) {
                b = generateRandomIndex();
            }
            Collections.swap(travel, a, b);
        }
        
        private int generateRandomIndex() {
            return (int) (travel.size() * Math.random());
        }

        public double getDistance() {
            return IntStream.rangeClosed(1, travel.size())
                    .mapToDouble(idx -> {
                        Node starting = travel.get(idx - 1);
                        Node destination = travel.get(idx == travel.size() ? 0 : idx);
                        return evaluator.getCost(starting, destination);
                    }).sum();
        }
        
        public Tour copy() {
            return new Tour(travel, evaluator);
        }
    }
    
    public static class Algo {

        public static Result simulateAnnealing(List<Node> cities, Config config) {
            final double coolingFactor = config.getCoolingFactor();
            if (coolingFactor > 1) {
                throw new RuntimeException("coolingFactor must be less than 1");
            }
            final double endTemperature = config.getEndTemperature();
            final double startTemperature = config.getStartTemperature();
            if (coolingFactor < 0 || endTemperature < 0 || startTemperature < 0) {
                throw new RuntimeException("coolingFactor, endTemperature amd startTemperature must be positive");
            }

            EstimateEvaluator<Double> evaluator = new PathFinding.GeoEstimateEvaluatorPointCustom(config.getPointProp());

            Tour current = new Tour(cities, evaluator);
            Tour best = current.copy();
            
            double temperature = config.getStartTemperature();
            while (temperature > config.getEndTemperature()) {
                Tour neighbor = current.copy();
                neighbor.swapCities();
                
                // Get distance of current and new (swapped) travel
                double currentDistance = current.getDistance();
                double newDistance = neighbor.getDistance();
                
                // Decide if we should accept the new result
                if (Math.random() < Math.exp((currentDistance - newDistance) / temperature)) {
                    current = neighbor.copy();
                }
                
                // Keep the best distance found
                if (current.getDistance() < best.getDistance()) {
                    best = current.copy();
                }

                // decrement temp via coolingFactor
                temperature *= config.getCoolingFactor();
            }
            
            // return virtual path result
            final List<VirtualNode> vNodes = best.travel.stream()
                    .map(VirtualNode::from)
                    .collect(Collectors.toList());

            final VirtualNode node = vNodes.get(0);
            final VirtualPath virtualPath = new VirtualPath(node);
            
            IntStream.range(0, best.travel.size() - 1)
                    .forEach(i -> {
                        final VirtualRelationship vRel = new VirtualRelationship(
                                vNodes.get(i), vNodes.get(i + 1), withName(config.getRelName()));
                        virtualPath.addRel(vRel);
                    });
            
            return new Result(virtualPath, best.getDistance());
        }
    }

    public static class Result {
        public Path path;
        public double distance;

        public Result(Path path, double distance) {
            this.path = path;
            this.distance = distance;
        }
    }

    public static class Config {
        public static final String POINT_PROP_KEY = "pointProp";
        
        private final Double coolingFactor;
        private final Double startTemperature;
        private final Double endTemperature;
        private final String pointProp;
        private final String latitudeProp;
        private final String longitudeProp;
        private final String relName;

        public Config(Map<String, Object> config) {
            if (config == null) config = Collections.emptyMap();
            this.coolingFactor = Util.toDouble(config.getOrDefault("coolingFactor", 0.995));
            this.startTemperature = Util.toDouble(config.getOrDefault("startTemperature", 100000));
            this.endTemperature = Util.toDouble(config.getOrDefault("endTemperature", 0.1));
            this.pointProp = (String) config.getOrDefault(POINT_PROP_KEY, "place");
            this.latitudeProp = (String) config.getOrDefault("latitudeProp", "latitude");
            this.longitudeProp = (String) config.getOrDefault("longitudeProp", "longitude");
            this.relName = (String) config.getOrDefault("relName", "CONNECT_TO");
        }

        public Double getCoolingFactor() {
            return coolingFactor;
        }

        public Double getStartTemperature() {
            return startTemperature;
        }

        public Double getEndTemperature() {
            return endTemperature;
        }

        public String getPointProp() {
            return pointProp;
        }

        public String getRelName() {
            return relName;
        }
    }
}
