"""Explain disputes without choosing an unsupported source or declaring real arrivals."""
from collections import defaultdict


def review_visits(documents, start, end):
    groups = defaultdict(list)
    for doc in documents:
        for call in doc["calls"]:
            day = call["localDate"]
            if day and start.isoformat() <= day <= end.isoformat() and call["island"] == "stThomas":
                groups[(day, call["canonicalVessel"])].append((doc, call))
    results = []
    for (day, vessel), records in sorted(groups.items()):
        calls = [call for _, call in records]
        reasons = []
        if len({c["dock"] for c in calls}) > 1:
            reasons.append("port_assignment_conflict")
        elif len({c.get("dockCode", c["rawDock"]) for c in calls}) > 1:
            reasons.append("berth_assignment_conflict")
        if len({(c["arrival"], c["departure"]) for c in calls if c["arrival"] and c["departure"]}) > 1:
            reasons.append("planned_time_conflict")
        statuses = {c.get("visitStatus", "listed") for c in calls}
        if len(statuses) > 1:
            reasons.append("visit_status_conflict")
        flags = sorted({f for c in calls for f in c["flags"]} | {
            "monthly_total_mismatch" for doc, _ in records
            if any(i["kind"] == "monthly_total_mismatch" and i.get("month") == day[:7]
                   for i in doc.get("issues", []))})
        status = "conflicting" if reasons else next(iter(statuses))
        if status == "listed" and flags:
            status = "needsReview"
        candidates = [{"sourceName": doc["provider"].upper(), "sourceURL": doc["url"],
                       "documentID": doc["id"], "documentHash": doc["sha256"],
                       "sourcePage": call.get("provenance", {}).get("page"),
                       "sourceRow": call.get("provenance", {}).get("row"),
                       "originalVesselName": call["originalVesselName"],
                       "dock": call["dock"], "rawDock": call["rawDock"],
                       "arrival": call["arrival"], "departure": call["departure"],
                       "sourceVisitStatus": call.get("visitStatus", "listed")}
                      for doc, call in records]
        results.append({"id": day + "|" + vessel, "date": day, "vessel": vessel, "visitStatus": status,
                        "conflictReasons": reasons, "reviewFlags": flags,
                        "sourceRowCount": len(candidates), "candidates": candidates,
                        "reviewState": "unreviewed"})
    return results
