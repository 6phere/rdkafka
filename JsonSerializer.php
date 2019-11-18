<?php

declare(strict_types=1);

namespace Enqueue\RdKafka;

class JsonSerializer implements Serializer
{
    public function toString(RdKafkaMessage $message): string
    {

        if ($message->getBody() != null && $message->getBody() != '' && isset(json_decode($message->getBody())->job)) {
            $json = json_encode([
                'body' => unserialize(json_decode($message->getBody())->data->command),
                'properties' => $message->getProperties(),
                'headers' => $message->getHeaders(),
            ]);
        } else {
            $json = json_encode([
                'body' => $message->getBody(),
                'properties' => $message->getProperties(),
                'headers' => $message->getHeaders(),
            ]);
        }

        if (JSON_ERROR_NONE !== json_last_error()) {
            throw new \InvalidArgumentException(sprintf(
                'The malformed json given. Error %s and message %s',
                json_last_error(),
                json_last_error_msg()
            ));
        }

        return $json;
    }

    public function toMessage(string $string, $config): RdKafkaMessage
    {
        $procesor = (array_key_exists('processor', $config) ? $config['processor'] : 'ProcessKafkaMessage') . "@handle";
        $body = [
            "job" => $procesor,
            "data" => $string
        ];
        return new RdKafkaMessage(json_encode($body), [], []);
    }
}
