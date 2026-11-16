
class RiskEngine:

    def __init__(self, threshold=0.5):
        self.threshold = threshold

    def evaluate(self, prediction, behaviour):

        probability = float(prediction["fraud_probability"])

        flags = []

        if not behaviour.get("history_available", False):
            flags.append("BEHAVIOURAL_HISTORY_UNAVAILABLE")

        else:
            ratio = behaviour.get("amount_ratio")
            time_gap = behaviour.get("time_since_previous")

            if ratio is not None and ratio >= 3:
                flags.append("AMOUNT_3X_PREVIOUS_AVERAGE")

            if time_gap is not None and 0 <= time_gap <= 60:
                flags.append("TRANSACTION_WITHIN_60_SECONDS")

        # The trained model remains the primary decision-maker.
        predicted_fraud = int(probability >= self.threshold)

        risk_level = (
            "HIGH" if predicted_fraud else "NORMAL"
        )

        return {
            "TransactionID": prediction["TransactionID"],
            "fraud_probability": probability,
            "predicted_fraud": predicted_fraud,
            "risk_level": risk_level,
            "review_recommended": bool(predicted_fraud),
            "behavioural_flags": flags,
            "ground_truth": prediction.get("ground_truth"),
            "replay_timestamp": prediction.get("replay_timestamp")
        }
