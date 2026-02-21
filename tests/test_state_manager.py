import unittest

from bet365.state_manager import OddsStateManager


class OddsStateManagerTests(unittest.TestCase):
    def test_topic_load_routes_and_updates_state(self):
        manager = OddsStateManager()

        manager.apply_message(
            {
                "type": "TOPIC_LOAD",
                "topic": "__time",
                "data": "F|IN;TI=20260220013254042;UF=55;|",
            }
        )

        self.assertEqual(manager.handled_messages, 1)
        self.assertEqual(manager.ignored_messages, 0)
        self.assertEqual(manager.unknown_types, 0)

        self.assertIn("__time", manager.topics)
        topic_state = manager.topics["__time"]
        self.assertEqual(topic_state.topic, "__time")
        self.assertEqual(topic_state.raw_payload, "F|IN;TI=20260220013254042;UF=55;|")
        self.assertEqual(topic_state.update_count, 1)
        self.assertEqual(topic_state.entities["key_values"]["TI"], "20260220013254042")
        self.assertEqual(topic_state.entities["key_values"]["UF"], "55")

    def test_topic_load_replaces_snapshot_entities(self):
        manager = OddsStateManager()

        manager.apply_message(
            {
                "type": "TOPIC_LOAD",
                "topic": "__time",
                "data": "F|IN;A=1;B=2;|",
            }
        )
        manager.apply_message(
            {
                "type": "TOPIC_LOAD",
                "topic": "__time",
                "data": "F|IN;C=3;|",
            }
        )

        topic_state = manager.topics["__time"]
        self.assertEqual(topic_state.update_count, 2)
        self.assertEqual(topic_state.raw_payload, "F|IN;C=3;|")
        self.assertEqual(topic_state.entities["key_values"], {"C": "3"})

    def test_control_messages_are_ignored(self):
        manager = OddsStateManager()

        manager.apply_message({"type": "CONFIG_100", "payload": "abc"})
        manager.apply_message({"type": "HANDSHAKE_RESPONSE", "payload": "ok"})

        self.assertEqual(manager.handled_messages, 0)
        self.assertEqual(manager.ignored_messages, 2)
        self.assertEqual(manager.unknown_types, 0)
        self.assertEqual(manager.topics, {})

    def test_unknown_and_malformed_messages_count_as_unknown(self):
        manager = OddsStateManager()

        manager.apply_message({"type": "SOMETHING_NEW", "payload": "x"})
        manager.apply_message({"type": "TOPIC_LOAD", "topic": "__time"})

        self.assertEqual(manager.handled_messages, 0)
        self.assertEqual(manager.ignored_messages, 0)
        self.assertEqual(manager.unknown_types, 2)

    def test_topic_metadata_changes_and_increments(self):
        manager = OddsStateManager()

        manager.apply_message(
            {
                "type": "TOPIC_LOAD",
                "topic": "__time",
                "data": "F|IN;TI=1;UF=55;|",
            }
        )
        first_state = manager.topics["__time"]
        first_update_ts = first_state.last_update_utc
        self.assertEqual(first_state.update_count, 1)

        manager.apply_message(
            {
                "type": "DELTA",
                "topic": "__time",
                "diff": "IN;TI=2;",
            }
        )
        second_state = manager.topics["__time"]

        self.assertEqual(second_state.update_count, 2)
        self.assertGreaterEqual(second_state.last_update_utc, first_update_ts)

    def test_delta_patches_existing_key_values(self):
        manager = OddsStateManager()

        manager.apply_message(
            {
                "type": "TOPIC_LOAD",
                "topic": "__time",
                "data": "F|IN;TI=1;UF=55;|",
            }
        )

        manager.apply_message(
            {
                "type": "DELTA",
                "topic": "__time",
                "diff": "IN;TI=2;NEW=abc;|",
            }
        )

        topic_state = manager.topics["__time"]
        self.assertEqual(topic_state.entities["key_values"]["TI"], "2")
        self.assertEqual(topic_state.entities["key_values"]["UF"], "55")
        self.assertEqual(topic_state.entities["key_values"]["NEW"], "abc")
        self.assertGreaterEqual(manager.delta_ops_applied, 2)

    def test_delta_before_topic_load_creates_topic_and_applies_upserts(self):
        manager = OddsStateManager()

        manager.apply_message(
            {
                "type": "DELTA",
                "topic": "__time",
                "diff": "TI=2;UF=55;",
            }
        )

        self.assertIn("__time", manager.topics)
        topic_state = manager.topics["__time"]
        self.assertEqual(topic_state.entities["key_values"], {"TI": "2", "UF": "55"})

    def test_delta_tracks_unparsed_tokens(self):
        manager = OddsStateManager()

        manager.apply_message(
            {
                "type": "TOPIC_LOAD",
                "topic": "__time",
                "data": "F|IN;TI=1;|",
            }
        )
        manager.apply_message(
            {
                "type": "DELTA",
                "topic": "__time",
                "diff": "IN;FLAG;TI=3;",
            }
        )

        topic_state = manager.topics["__time"]
        self.assertEqual(topic_state.entities["key_values"]["TI"], "3")
        self.assertIn("delta_unparsed_tokens", topic_state.entities)
        self.assertIn("IN", topic_state.entities["delta_unparsed_tokens"])
        self.assertIn("FLAG", topic_state.entities["delta_unparsed_tokens"])
        self.assertGreaterEqual(manager.delta_ops_skipped, 2)

    def test_stale_delta_by_topic_time_is_dropped(self):
        manager = OddsStateManager()

        manager.apply_message(
            {
                "type": "TOPIC_LOAD",
                "topic": "__time",
                "data": "F|IN;TI=20260221010101001;UF=55;|",
            }
        )

        manager.apply_message(
            {
                "type": "DELTA",
                "topic": "__time",
                "diff": "TI=20260221010101000;UF=99;",
            }
        )

        topic_state = manager.topics["__time"]
        self.assertEqual(topic_state.entities["key_values"]["TI"], "20260221010101001")
        self.assertEqual(topic_state.entities["key_values"]["UF"], "55")
        self.assertEqual(topic_state.stale_updates_dropped, 1)
        self.assertEqual(manager.out_of_order_dropped, 1)
        self.assertEqual(manager.stale_by_topic_time, 1)
        self.assertEqual(topic_state.last_error, "stale_by_topic_time")

    def test_malformed_known_message_increments_malformed_counter(self):
        manager = OddsStateManager()

        manager.apply_message({"type": "DELTA", "topic": "__time"})

        self.assertEqual(manager.handled_messages, 0)
        self.assertEqual(manager.malformed_messages, 1)
        self.assertEqual(manager.unknown_types, 1)


if __name__ == "__main__":
    unittest.main()
