import ast
import json
import aql


conn = aql.Connection("http://localhost:8201")
O = conn.graph("bmeg-test")

# shortcut, because this is going to be used a lot.
isa = isinstance


def parse_expr(raw):
    mod = ast.parse(raw)
    expr = mod.body[0]
    if not isinstance(expr, ast.Expr):
        raise Exception("non expression")
    return visit(expr.value)


def require_value(node):
    if isa(node, ast.Num):
        return node.n
    if isa(node, ast.Str):
        return node.s
    if isa(node, ast.List):
        # TODO not the best. could be a list containing names or function calls.
        return ast.literal_eval(node)
    raise Exception("value type required")
    

def isa_name(node):
    return isa(node, ast.Name) or isa(node, ast.Attribute)


def get_name(node):
    if isa(node, ast.Name):
        return node.id

    if isa(node, ast.Attribute):
        # TODO super hacky
        return '.'.join([node.value.id, node.attr])

    raise Exception("couldn't get name")


def visit(node):
    print(ast.dump(node))

    if isa(node, ast.UnaryOp):
        if not isa(node.op, ast.Not):
            raise Exception("unsupported unary op")
        return aql.not_(sub)

    if isa(node, ast.BoolOp):
        if isa(node.op, ast.And):
            sub = [visit(v) for v in node.values]
            return aql.and_(sub)

        if isa(node.op, ast.Or):
            sub = [visit(v) for v in node.values]
            return aql.or_(sub)

    if isa(node, ast.Compare):
        if len(node.ops) > 1:
            # TODO this is possible, just not done yet
            raise Exception("unimplemented range comparison")

        if len(node.ops) == 1:

            op = node.ops[0]
            left = node.left

            if len(node.comparators) != 1:
                raise Exception("unhandled multiple comparators")

            right = node.comparators[0]

            if isa(left, ast.Name) and isa(right, ast.Name):
                # TODO would it be cool if arachne allowed this?
                raise Exception("comparing two names")

            if isa_name(left):
                key = get_name(left)
                val = require_value(right)
            elif isa_name(right):
                key = get_name(right)
                val = require_value(left)
            else:
                raise Exception("can't find name in comparison")
        
            if isa(op, ast.In):
                if isa(left, ast.Name):
                    return aql.in_(key, val)
                elif isa(right, ast.Name):
                    return aql.contains(key, val)

            if isa(op, ast.NotIn):
                # TODO not sure arachne supports this
                raise Exception("unimplemented")
                #return aql.in_(key, val)
        
            if isa(op, ast.Gt):
                return aql.gt(key, val)

            if isa(op, ast.GtE):
                return aql.gte(key, val)
        
            if isa(op, ast.Lt):
                return aql.lt(key, val)

            if isa(op, ast.LtE):
                return aql.lte(key, val)
        
            if isa(op, ast.Eq) or isa(op, ast.Is):
                return aql.eq(key, val)

            if isa(op, ast.NotEq) or isa(op, ast.IsNot):
                return aql.neq(key, val)

    raise Exception("unknown condition/state")



class Project:

    def __init__(self, expr=""):
        self.q = O.query().V().where(aql.eq("_label", "Project"))
        if expr:
            self.q = self.q.where(parse_expr(expr))

    @property
    def Individual(self, expr=""):
        q = self.q.in_("InProject")
        if expr:
            q = q.where(parse_expr(expr))
        i = Individual.__new__(Individual)
        i.q = q
        return i

    def execute(self):
        return self.q.execute()


class Individual:

    def __init__(self, expr=""):
        self.q = O.query().V().where(aql.eq("_label", "Individual"))
        if expr:
            self.q = self.q.where(parse_expr(expr))

    @property
    def Biosample(self, expr=""):
        q = self.q.in_("BiosampleFor")
        if expr:
            q = q.where(parse_expr(expr))
        b = Biosample.__new__(Biosample)
        b.q = q
        return b

    @property
    def Project(self, expr=""):
        q = self.q.out("InProject")
        if expr:
            q = q.where(parse_expr(expr))
        p = Project.__new__(Project)
        p.q = q
        return p

    def execute(self):
        return self.q.execute()


class Biosample:

    def __init__(self, expr=""):
        self.q = O.query().V().where(aql.eq("_label", "Biosample"))
        if expr:
            self.q = self.q.where(parse_expr(expr))

    def __call__(self, expr=""):
        if expr:
            self.q = self.q.where(parse_expr(expr))
        return self

    @property
    def Individual(self, expr=""):
        q = self.q.out("BiosampleFor")
        if expr:
            q = q.where(parse_expr(expr))
        i = Individual.__new__(Individual)
        i.q = q
        return i

    def execute(self):
        #print(json.dumps(self.q.query, indent=True))
        return self.q.execute()


#print(parse_expr("project is 'TCGA-BRCA'"))
print(Project("project_id is 'TCGA-BRCA'").Individual.Biosample("gdc_attributes.sample_type is 'Primary Tumor'").execute())
#print(Project("project_id is 'TCGA-BRCA'").execute())
#print(Biosample("ccle_attributes.Source is 'ATCC'").execute())

