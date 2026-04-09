"""
Edge Case Handler — catches non-analytical queries BEFORE hitting Gemini API.
Zero cost. Instant response. Professional tone.
"""
import re, logging

logger = logging.getLogger(__name__)

# ── Pattern banks ─────────────────────────────────────────────

_GREETINGS = [
    r"^(hi|hello|hey|hii+|helo|namaste|namaskar|vanakkam|good\s*(morning|afternoon|evening|day|night))[\s!.?]*$",
    r"^(howdy|greetings|sup|whats?\s*up|yo|hola|bonjour|salaam)[\s!.?]*$",
    r"^(good\s*morning|good\s*afternoon|good\s*evening|good\s*night)[\s!.?]*$",
]

_IDENTITY = [
    r"(who|what)\s+(are|r)\s+(you|u)",
    r"(your|ur)\s+name",
    r"are\s+you\s+(ai|bot|human|real|chatbot|robot|machine|gpt|claude|gemini)",
    r"(tell|about)\s+(me\s+)?about\s+(yourself|you)",
    r"what\s+can\s+you\s+do",
    r"what\s+do\s+you\s+do",
    r"(introduce)\s+(yourself)",
    r"which\s+(ai|model|llm|technology)\s+(are|is|do)\s+you",
    r"(powered|built|made|developed|created)\s+by",
    r"are\s+you\s+better\s+than",
]

_THANKS = [
    r"^(thanks?|thank\s*you|ty|thx|thanku|dhanyavaad|dhanyawad|shukriya)[\s!.?]*$",
    r"^(that.?s?\s+(great|helpful|perfect|awesome|nice|good|excellent|wonderful))[\s!.?]*$",
    r"^(great|perfect|awesome|excellent|wonderful|brilliant|fantastic)[\s!.?]*$",
]

_GOODBYE = [
    r"^(bye|goodbye|good\s*bye|see\s*you|tata|alvida|cya|take\s*care|later)[\s!.?]*$",
    r"^(have\s+a\s+(good|great|nice|wonderful)\s+(day|evening|night))[\s!.?]*$",
]

_SILLY = [
    # Jokes and entertainment
    r"(tell|say)\s+(me\s+)?(a\s+)?joke",
    r"(tell|say)\s+(me\s+)?(a\s+)?funny",
    r"make\s+me\s+laugh",
    r"sing\s+(a\s+)?song",
    r"write\s+(a\s+)?(poem|story|essay|rap|song|lyrics)",
    r"(play|let.*play)\s+(a\s+)?game",

    # Personal feelings
    r"do\s+you\s+(like|love|hate|feel|think|dream|sleep|eat|drink|breathe)",
    r"(favorite|favourite)\s+(color|colour|food|movie|song|book|sport|animal)",
    r"how\s+old\s+are\s+you",
    r"where\s+do\s+you\s+(live|stay|come\s+from)",
    r"are\s+you\s+(happy|sad|angry|tired|bored|excited|hungry|scared)",
    r"(marry|date|love|kiss|hug)\s+me",
    r"will\s+you\s+(marry|date|be\s+my)",
    r"do\s+you\s+have\s+(feelings|emotions|heart|soul|family|friends)",
    r"can\s+you\s+(feel|dream|cry|laugh|love|dance)",

    # Philosophical / random
    r"what\s+is\s+the\s+(meaning|purpose)\s+of\s+(life|everything)",
    r"(is\s+god|does\s+god)\s+(real|exist)",
    r"what\s+(happens|comes)\s+after\s+death",
    r"who\s+created\s+the\s+(world|universe)",

    # Comparisons with other AI
    r"(better|worse)\s+than\s+(chatgpt|gpt|openai|claude|gemini|copilot)",
    r"vs\s+(chatgpt|gpt|claude|gemini|copilot|bard)",

    # Random tasks
    r"translate\s+(this|to|into)\s+",
    r"(write|draft|compose)\s+(a\s+)?(email|letter|message|whatsapp|cv|resume)",
    r"solve\s+(this\s+)?(math|equation|problem|puzzle)",
    r"what\s+is\s+\d+\s*[\+\-\*\/]\s*\d+",  # math calculations
    r"(predict|forecast)\s+(future|stock|crypto|price|weather)",
    r"give\s+me\s+(advice|tips)\s+on\s+(life|love|money|career|health)",

    # Insults / rude
    r"(stupid|dumb|useless|idiot|fool|trash|garbage)\s*(bot|ai|system|app)?",
    r"you\s+(suck|are\s+bad|are\s+useless|are\s+dumb)",

    # General knowledge — geography, science, history, politics
    r"(capital|president|prime\s*minister|currency|population|area)\s+of\s+\w+",
    r"what\s+is\s+the\s+(capital|currency|language|population|flag)\s+of",
    r"who\s+(is|was)\s+(the\s+)?(president|prime\s*minister|king|queen|ceo|founder|inventor)",
    r"(largest|smallest|tallest|longest|biggest|fastest|richest|poorest)\s+(country|city|river|mountain|building)",
    r"when\s+(was|did|is)\s+.*(born|invented|discovered|founded|independence|war|battle)",
    r"(define|definition|meaning)\s+of\s+(?!dssy|dayanand|eligib|beneficiar|pension)",
    r"(what|how)\s+(is|does|do)\s+(gravity|photosynthesis|evolution|democracy|inflation|climate)",
    r"(recipe|ingredients|how\s+to\s+cook|how\s+to\s+make)\s+",
    r"(symptom|treatment|cure|medicine|doctor|hospital)\s+(for|of)\s+",
    r"(ipl|cricket|football|match|score|team|player|tournament)\b",
    r"(bollywood|hollywood|movie|film|actor|actress|tv\s*series)\b",
]

_CONFUSED = [
    r"^(i\s+don.?t\s+(know|understand)|what|huh|what\?|confused|i\s*m\s+confused)[\s!.?]*$",
    r"^(help|help\s*me|i\s+need\s+help)[\s!.?]*$",
    r"^(hmm+|umm+|ok+|okay|k|yes|no|yeah|nah|sure|right|got\s*it)[\s!.?]*$",
    r"^(start|begin|let.s\s*(start|begin|go))[\s!.?]*$",
    r"^(what\s+next|now\s+what|then\s+what|so\s+what)[\s!.?]*$",
    r"^\?+$",  # just question marks
]

_OFF_TOPIC = [
    r"(weather|cricket|football|soccer|movie|film|song|recipe|news|horoscope)\b",
    r"(stock|share|market|bitcoin|crypto|nft|investment)\b",
    r"(train|flight|bus|ticket|booking|hotel|travel|visa)\b",
    r"(exam|result|admission|school|college|university|job|interview)\b",
    r"(cooking|food|restaurant|diet|exercise|workout|gym)\b",
    r"(politics|election|party|minister|parliament)\b",
    r"(amazon|flipkart|shopping|buy|sell|price\s+of)\b",
]

_PROFANITY_REDIRECT = [
    r"\b(fuck|shit|damn|bastard|hell|crap|ass)\b",
]

# ── Response templates ────────────────────────────────────────

_SUGGESTION_BLOCK = (
    "\n\nYou can ask me things like:\n"
    "  \"How many active beneficiaries are in Bardez taluka?\"\n"
    "  \"Compare North Goa vs South Goa beneficiaries\"\n"
    "  \"What documents are required to apply for DSSS?\"\n"
    "  \"Show category-wise monthly payout\""
)

_RESPONSES = {
    "greeting": (
        "Namaste! I'm the DSSS Statistical Analysis Assistant for the "
        "Department of Social Welfare, Government of Goa. I can help you with:\n\n"
        "Beneficiary statistics — district, taluka, category-wise counts\n"
        "Scheme information — eligibility, financial assistance, application process\n"
        "Analytical queries — trends, comparisons, payment compliance\n\n"
        "How may I assist you today?"
    ),
    "identity": (
        "I am the **Neural AI Governance** system — a statistical analysis assistant "
        "built specifically for the Dayanand Social Security Scheme (DSSS), "
        "Department of Social Welfare, Government of Goa.\n\n"
        "My capabilities:\n"
        "  Real-time DSSS beneficiary database queries\n"
        "  District, taluka, village, and category-wise analytics\n"
        "  Scheme eligibility, documents, and financial assistance information\n"
        "  Payment compliance and trend analysis\n\n"
        "I am not a general-purpose chatbot — I'm purpose-built for DSSS governance. "
        "How may I help you?"
    ),
    "thanks": (
        "You're welcome! Feel free to ask any other questions about DSSS "
        "beneficiaries, scheme eligibility, or statistics."
    ),
    "goodbye": (
        "Thank you for using the DSSS AI Query System. Have a great day! "
        "You can return anytime for beneficiary statistics or scheme information."
    ),
    "silly": (
        "That's an interesting question, but I'm a **purpose-built assistant** "
        "for the Dayanand Social Security Scheme (DSSS), Government of Goa — "
        "so general topics are a bit outside my expertise!\n\n"
        "Could you try asking me something like:\n"
        "  \"How many active beneficiaries are in North Goa?\"\n"
        "  \"Show category-wise monthly payout as a chart\"\n"
        "  \"What are the eligibility criteria for DSSS?\"\n"
        "  \"Which taluka has the most senior citizens?\"\n\n"
        "I'd be happy to help with any DSSS statistics or scheme information!"
    ),
    "profanity": (
        "I understand you may be frustrated, and I'm here to help! "
        "I specialise in DSSS scheme queries and beneficiary data for the "
        "Department of Social Welfare, Government of Goa.\n\n"
        "Let's try again — what would you like to know about DSSS? "
        "For example: \"How many beneficiaries are there in South Goa?\" "
        "or \"What documents are required to apply?\""
    ),
    "confused": (
        "No worries! Here are some things you can ask me:\n\n"
        "**Statistical & Visualization Queries:**\n"
        "  \"How many total beneficiaries are there?\"\n"
        "  \"Show taluka-wise active beneficiary count\"\n"
        "  \"Compare North Goa vs South Goa beneficiaries\"\n"
        "  \"What is the gender-wise breakdown?\"\n"
        "  \"Which category has the highest payout?\"\n\n"
        "**Scheme Information:**\n"
        "  \"What are the eligibility criteria for DSSS?\"\n"
        "  \"How much pension do senior citizens receive?\"\n"
        "  \"What documents are needed to apply?\"\n"
        "  \"What is the Life Certificate requirement?\"\n\n"
        "Just type your question and I'll get right on it!"
    ),
    "off_topic": (
        "That's a great question — but it's a little outside my area! "
        "I'm a **DSSS Statistical Analysis Assistant** built exclusively for "
        "the Dayanand Social Security Scheme, Department of Social Welfare, Government of Goa.\n\n"
        "Could you ask me something related to DSSS? For example:\n\n"
        "  \"How many widow beneficiaries are there in Goa?\"\n"
        "  \"Show me a district-wise breakdown with a chart\"\n"
        "  \"What is the income limit to qualify for DSSS?\"\n"
        "  \"Which taluka has the highest number of disabled beneficiaries?\"\n"
        "  \"How much pension do senior citizens receive per month?\"\n\n"
        "I can run live database queries, generate visualizations, and answer "
        "all scheme-related questions. How can I help you with DSSS today?"
    ),
}


def detect_edge_case(question: str) -> dict | None:
    """
    Returns {type, response} if edge case detected.
    Returns None if legitimate SQL/RAG query.
    """
    q = question.strip()
    if len(q) < 2:
        return {"type": "confused", "response": _RESPONSES["confused"]}

    ql = q.lower()

    # ── Early exit: clear DSSS analytical intent → skip all edge checks ──
    _DSSY_STRONG = [
        r"\bbeneficiar", r"\btaluka\b", r"\bdistrict\b", r"\bdssy\b", r"\bdsss\b",
        r"\bactive\b", r"\binactive\b", r"\bdeceased\b",
        r"\bpension\b", r"\bpayout\b", r"\bpayment\b",
        r"\bscheme\b", r"\beligib", r"\bwidow", r"\bdisabled\b",
        r"\bsenior\s*citizen", r"\bcategory\b", r"\bregistration\b",
        r"(north|south)\s*goa", r"\bgoa\b.*\b(count|total|how many)",
        r"\bsocial\s*welfare\b", r"\bfinancial\s*assistance\b",
        r"\bdashboard\b", r"\benroll", r"\bstatus\s*histor",
        r"\bfiscal\b", r"\bbatch\b", r"\blife\s*cert",
        r"\bcompliance\b", r"\bvillage\b", r"\bofficer\b",
    ]
    if any(re.search(p, ql) for p in _DSSY_STRONG):
        return None

    # Order matters — most specific first
    for pattern in _GREETINGS:
        if re.search(pattern, ql):
            return {"type": "greeting", "response": _RESPONSES["greeting"]}

    for pattern in _IDENTITY:
        if re.search(pattern, ql):
            return {"type": "identity", "response": _RESPONSES["identity"]}

    for pattern in _THANKS:
        if re.search(pattern, ql):
            return {"type": "thanks", "response": _RESPONSES["thanks"]}

    for pattern in _GOODBYE:
        if re.search(pattern, ql):
            return {"type": "goodbye", "response": _RESPONSES["goodbye"]}

    for pattern in _PROFANITY_REDIRECT:
        if re.search(pattern, ql):
            return {"type": "profanity", "response": _RESPONSES["profanity"]}

    for pattern in _SILLY:
        if re.search(pattern, ql):
            return {"type": "silly", "response": _RESPONSES["silly"]}

    for pattern in _CONFUSED:
        if re.search(pattern, ql):
            return {"type": "confused", "response": _RESPONSES["confused"]}

    # ── Meta-conversation passthrough ─────────────────────────
    # Questions about the conversation itself (context-aware) must reach
    # resolve_question() — they have no DSSS keywords by nature.
    _META_CONV = [
        r"\b(my|your)\s+(first|last|previous|prior|earlier|last)\s+(question|query|message)",
        r"what\s+did\s+(i|you)\s+(ask|say|answer|tell)",
        r"what\s+was\s+(my|your|the)\s+(question|answer|response|last|first)",
        r"(repeat|rephrase|restate)\s+(my|the|that|your)\s+(question|answer)",
        r"(what|which)\s+(question|thing)\s+did\s+i\s+(ask|say)",
        r"(summarize|summary)\s+(our|this|the)\s+(conversation|chat|discussion)",
        r"what\s+have\s+(we|i)\s+(discussed|talked|covered)",
    ]
    if any(re.search(p, ql) for p in _META_CONV):
        return None

    # ── Arithmetic / comparison follow-up passthrough ──────────
    # Short follow-ups that reference prior answer numbers via pronouns
    # ("both", "them", "together", "combined") have no DSSS keywords
    # but must reach resolve_question() to produce coherent answers.
    _FOLLOWUP = [
        # Arithmetic on prior numbers
        r"\b(sum|total|add|plus|combined|combine|altogether)\b.{0,30}\b(both|them|these|those|two|it)\b",
        r"\b(both|them|these|those)\b.{0,30}\b(sum|total|add|plus|combined|together|altogether)\b",
        r"(what|how much).{0,20}(together|combined|altogether|in total)\b",
        r"\b(difference|gap|subtract|minus)\b.{0,30}\b(both|them|these|those|two)\b",
        # Comparison of prior results
        r"which\s+(is|one\s+is|are|has)\s+(the\s+)?(more|most|less|least|higher|highest|lower|lowest|bigger|biggest|smaller|smallest|greater|greatest|maximum|minimum|max|min)\b",
        r"(more|less|higher|lower|bigger|smaller|greater)\s+(of\s+)?(the\s+)?(two|both|them|these)\b",
        # Superlative / ranking follow-ups on prior result
        r"\b(highest|lowest|most|least|maximum|minimum|max|min|top|bottom|first|last|largest|smallest)\b.{0,20}\b(in\s+this|of\s+these|from\s+(this|above|that|the)|among|one)\b",
        r"\b(in\s+this|from\s+(this|above|that|the)|of\s+these|among\s+these)\b.{0,20}\b(highest|lowest|most|least|maximum|minimum|max|min|top|bottom|largest|smallest)\b",
        r"^(which|what|who).{0,30}(highest|lowest|most|least|maximum|minimum|top|bottom|largest|smallest)\b",
        # Short pronoun-only follow-ups ("what about both?", "and them?")
        r"^(what\s+about|and|also|plus)\s+(both|them|these|those|it|the\s+other)[\s?]*$",
        # "now show both", "combine them", "add those up"
        r"^(now\s+)?(show|give|tell|calculate|compute|find|sort|order|rank)\s+(me\s+)?(both|them|the\s+total|the\s+sum|the\s+combined|by|in)\b",
        r"^(add|sum|combine|total)\s+(them|both|those|these)\s*(up)?[\s?]*$",
        # Generic follow-ups referencing prior context ("in this", "from above", "of these")
        r"^.{0,15}\b(in\s+this|from\s+(this|above|that|the\s+above)|of\s+these|from\s+these)\b",
        # "and the X?", "what about X?" — short contextual follow-ups
        r"^(and|but)\s+(the|what)\b.{0,40}$",
        r"^what\s+about\s+.{2,30}$",
        # Contextual follow-ups: explain, why, reason, correct, right, wrong
        r"^(explain|why|reason|correct|right|wrong|is\s+this|is\s+that|you\s+give|you\s+gave)\b",
        r"\b(explain|why)\s+(this|that|the|above|it|these|those|the\s+above)\b",
        r"\b(is\s+this|is\s+that|is\s+it)\s+(correct|right|wrong|true|false|accurate)\b",
        r"\b(this|that|the\s+above|previous|prior|earlier)\s+(is|was|seems?|looks?)\s+(correct|right|wrong|off|incorrect)\b",
        r"\b(you\s+give|you\s+gave|you\s+said|you\s+told|you\s+mentioned)\b",
        r"\byour\s+(opinion|thought|analysis|view|answer|response)\b",
        r"^(yes|no|right|correct|wrong|exactly|not\s+right|that.?s\s+(right|wrong|correct|incorrect))\b",
    ]
    if any(re.search(p, ql) for p in _FOLLOWUP):
        return None

    # ── Non-English passthrough ──────────────────────────────────
    # If the question contains significant non-ASCII characters (Hindi,
    # Telugu, Kannada, Marathi, Konkani), let it through — the LLM
    # understands these languages and can answer DSSS questions in them.
    non_ascii_count = sum(1 for c in q if ord(c) > 127)
    if non_ascii_count >= 3:
        return None

    # Whitelist: ONLY allow questions related to DSSS context
    # If none of these words are present — it's off-topic, block it
    dssy_words = [
        # Scheme names
        "dssy", "dsss", "dayanand", "social security",
        # Beneficiary terms (including common misspellings)
        "beneficiar", "benificiar", "beneficier", "benficiar",
        "pension", "scheme", "welfare",
        # Geography
        "taluka", "district", "goa", "bardez", "salcete", "tiswadi",
        "bicholim", "pernem", "sattari", "canacona", "quepem", "sanguem",
        "mormugao", "dharbandora", "ponda", "north goa", "south goa", "panaji", "margao",
        # Categories
        "senior", "widow", "widows", "disabled", "hiv", "aids", "single woman",
        "category", "categories", "leprosy", "deaf", "cancer", "kidney", "sickle",
        # Financial
        "payment", "payout", "monthly amount", "financial assistance",
        r"rs\.", "rupee", "lakh", "crore", "batch", "ecs", "disburs",
        # Admin
        "eligible", "eligib", "registration", "active", "inactive", "deceased",
        "village", "life certificate", "aadhaar", "documents", "apply",
        "director", "social welfare", "amendment", "grievance", "officer",
        # Analytics & Dashboard
        "count", "total", "how many", "show", "list", "compare",
        "trend", "distribution", "breakdown", "compliance", "gender",
        "age", "statistic", "analytic", "data", "report",
        "dashboard", "chart", "visualization", "graph", "dynamic",
        # New tables
        "enrollment", "enrolled", "status history", "status change",
        "fiscal year", "fiscal period", "quarter", "amount history",
        "category transfer",
        # Year/date references (for queries like "year range", "2024 to 2026")
        "year", "month", "date", "range", "period", "from.*to",
        r"\b20\d{2}\b",
    ]
    has_dssy_context = any(re.search(w, ql) for w in dssy_words)

    # If question has no DSSS context at all — block it
    if not has_dssy_context:
        return {"type": "off_topic", "response": _RESPONSES["off_topic"]}

    return None