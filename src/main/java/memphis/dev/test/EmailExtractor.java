package memphis.dev.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EmailExtractor implements ValueMapper<String, String> {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final Pattern emailPattern = Pattern.compile("\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}\\b");

    @Override
    public String apply(String json) {
        try {
            JsonNode jsonNode = objectMapper.readTree(json);
            List<String> emails = extractEmails(jsonNode);
            return addEmailsToEvent(jsonNode, emails);
        } catch (IOException e) {
            throw new RuntimeException("Error processing JSON", e);
        }
    }

    private List<String> extractEmails(JsonNode jsonNode) {
        List<String> emails = new ArrayList<>();
        extractEmailsFromNode(jsonNode, emails);
        return emails;
    }

    private void extractEmailsFromNode(JsonNode jsonNode, List<String> emails) {
        if (jsonNode.isObject()) {
            Iterator<JsonNode> elements = jsonNode.elements();
            while (elements.hasNext()) {
                extractEmailsFromNode(elements.next(), emails);
            }
        } else if (jsonNode.isArray()) {
            for (JsonNode node : jsonNode) {
                extractEmailsFromNode(node, emails);
            }
        } else if (jsonNode.isTextual()) {
            Matcher matcher = emailPattern.matcher(jsonNode.asText());
            while (matcher.find()) {
                emails.add(matcher.group());
            }
        }
    }

    private String addEmailsToEvent(JsonNode jsonNode, List<String> emails) {
        // Create a new JSON node with the added "emails" field
        ObjectNode modifiedNode = objectMapper.createObjectNode();
        modifiedNode.setAll((ObjectNode) jsonNode);
        modifiedNode.putPOJO("emails", emails);

        // Convert the modified node back to a JSON string
        try {
            return objectMapper.writeValueAsString(modifiedNode);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error converting JSON node to string", e);
        }
    }
}
