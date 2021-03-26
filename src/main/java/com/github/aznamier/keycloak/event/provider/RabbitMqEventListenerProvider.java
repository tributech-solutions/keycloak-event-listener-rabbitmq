package com.github.aznamier.keycloak.event.provider;

import java.util.HashMap;
import java.util.Map;

import org.jboss.logging.Logger;

import org.keycloak.events.Event;
import org.keycloak.events.EventType;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.events.Details;
import org.keycloak.models.UserModel;
import org.keycloak.models.KeycloakSession;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.BasicProperties.Builder;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitMqEventListenerProvider implements EventListenerProvider {

	private static Logger logger = Logger.getLogger(RabbitMqEventListenerProvider.class);
	private RabbitMqConfig cfg;
	private ConnectionFactory factory;
	private KeycloakSession session;

	public RabbitMqEventListenerProvider(RabbitMqConfig cfg, KeycloakSession session) {
		this.cfg = cfg;
		this.session = session;
		
		this.factory = new ConnectionFactory();

		this.factory.setUsername(cfg.getUsername());
		this.factory.setPassword(cfg.getPassword());
		this.factory.setVirtualHost(cfg.getVhost());
		this.factory.setHost(cfg.getHostUrl());
		this.factory.setPort(cfg.getPort());
	}

	@Override
	public void close() {

	}

	@Override
	public void onEvent(Event event) {
		// we need to enrich event with more user information for certain event types
		// see AccountRestService.java vs. AccountFormService.java
		// https://github.com/keycloak/keycloak/pull/7495
		if (event.getType().equals(EventType.UPDATE_PROFILE) || event.getType().equals(EventType.REGISTER)) {
			logger.debugv("Enrich data at {0} event in RabbitMqEventListener", event.getType().name());
			UserModel user = session.users().getUserById(event.getUserId(), session.getContext().getRealm());
			if (event.getDetails() == null) {
				event.setDetails(new HashMap<>());
			}
			event.getDetails().putIfAbsent(Details.USERNAME, user.getUsername());
			event.getDetails().putIfAbsent("updated_email", user.getEmail());
			event.getDetails().putIfAbsent("updated_first_name", user.getFirstName());
			event.getDetails().putIfAbsent("updated_last_name", user.getLastName());;
		}

		EventClientNotificationMqMsg msg = EventClientNotificationMqMsg.create(event);
		String routingKey = RabbitMqConfig.calculateRoutingKey(event);
		String messageString = RabbitMqConfig.writeAsJson(msg, true);
		
		BasicProperties msgProps = this.getMessageProps(EventClientNotificationMqMsg.class.getName());
		this.publishNotification(messageString, msgProps, routingKey);
	}

	@Override
	public void onEvent(AdminEvent event, boolean includeRepresentation) {
		EventAdminNotificationMqMsg msg = EventAdminNotificationMqMsg.create(event);
		String routingKey = RabbitMqConfig.calculateRoutingKey(event);
		String messageString = RabbitMqConfig.writeAsJson(msg, true);
		BasicProperties msgProps = this.getMessageProps(EventAdminNotificationMqMsg.class.getName());
		this.publishNotification(messageString,msgProps, routingKey);
	}
	
	private BasicProperties getMessageProps(String className) {
		
		Map<String,Object> headers = new HashMap<String,Object>();
		headers.put("__TypeId__", className);
		
		Builder propsBuilder = new AMQP.BasicProperties.Builder()
				.appId("Keycloak")
				.headers(headers)
				.contentType("application/json")
				.contentEncoding("UTF-8");
		return propsBuilder.build();
	}
	

	private void publishNotification(String messageString, BasicProperties props, String routingKey) {

		

		try {
			Connection conn = factory.newConnection();
			Channel channel = conn.createChannel();
			
			channel.basicPublish(cfg.getExchange(), routingKey, props, messageString.getBytes());
			System.out.println("keycloak-to-rabbitmq SUCCESS sending message: " + routingKey);
			channel.close();
			conn.close();

		} catch (Exception ex) {
			System.err.println("keycloak-to-rabbitmq ERROR sending message: " + routingKey);
			ex.printStackTrace();
		}
	}

}
