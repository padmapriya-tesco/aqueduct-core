package com.tesco.aqueduct.pipe.http.client;

import com.tesco.aqueduct.registry.PipeLoadBalancer;
import io.micronaut.context.annotation.Replaces;
import io.micronaut.discovery.DiscoveryClient;
import io.micronaut.http.client.LoadBalancer;
import io.micronaut.http.client.loadbalance.DiscoveryClientLoadBalancerFactory;

import javax.inject.Singleton;

@Singleton
@Replaces(DiscoveryClientLoadBalancerFactory.class)
public class PipeLoadBalancerFactory extends DiscoveryClientLoadBalancerFactory {

    public static final String SERVICE_ID = "pipe";
    private PipeLoadBalancer pipeLoadBalancer;

    public PipeLoadBalancerFactory(PipeLoadBalancer pipeLoadBalancer, DiscoveryClient discoveryClient) {
        super(discoveryClient);
        this.pipeLoadBalancer = pipeLoadBalancer;
    }

    @Override
    public LoadBalancer create(String serviceID) {
        if(serviceID.equals(SERVICE_ID)) {
            return pipeLoadBalancer;
        }
        return super.create(serviceID);
    }
}
