/*
 * Souffle - A Datalog Compiler
 * Copyright (c) 2013, 2014, Oracle and/or its affiliates. All rights reserved
 * Licensed under the Universal Permissive License v 1.0 as shown at:
 * - https://opensource.org/licenses/UPL
 * - <souffle root>/licenses/SOUFFLE-UPL.txt
 */

/************************************************************************
 *
 * @file RamCondition.h
 *
 * Defines a class for evaluating conditions in the Relational Algebra
 * Machine.
 *
 ***********************************************************************/

#pragma once

#include "BinaryConstraintOps.h"
#include "RamExpression.h"
#include "RamNode.h"
#include "RamRelation.h"
#include "SymbolTable.h"

#include <algorithm>
#include <queue>
#include <sstream>
#include <string>

#include <cstdlib>

namespace souffle {

/**
 * @class RamCondition
 * @brief Abstract class for conditions and boolean values in RAM
 */
class RamCondition : public RamNode {
public:
    RamCondition* clone() const override = 0;
};

/**
 * @class RamTrue
 * @brief True value condition
 *
 * Output is "true"
 */
class RamTrue : public RamCondition {
public:
    void print(std::ostream& os) const override {
        os << "true";
    }

    RamTrue* clone() const override {
        return new RamTrue();
    }
};

inline bool isRamTrue(const RamCondition* cond) {
    return nullptr != dynamic_cast<const RamTrue*>(cond);
}

/**
 * @class RamTrue
 * @brief False value condition
 *
 * Output is "false"
 */
class RamFalse : public RamCondition {
public:
    void print(std::ostream& os) const override {
        os << "false";
    }

    RamFalse* clone() const override {
        return new RamFalse();
    }
};

/**
 * @class RamConjunction
 * @brief A conjunction of conditions
 *
 * Condition of the form "LHS and RHS", where LHS
 * and RHS are conditions
 *
 * For example:
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * C1 AND C2 AND C3
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * Is a RamConjunction, which may have LHS "C1"
 * and RHS "C2 AND C3"
 */
class RamConjunction : public RamCondition {
public:
    RamConjunction(std::unique_ptr<RamCondition> l, std::unique_ptr<RamCondition> r)
            : lhs(std::move(l)), rhs(std::move(r)) {
        assert(lhs != nullptr && "left-hand side of conjunction is a nullptr");
        assert(rhs != nullptr && "right-hand side of conjunction is a nullptr");
    }

    /** @brief Get left-hand side of conjunction */
    const RamCondition& getLHS() const {
        return *lhs;
    }

    /** @brief Get right-hand side of conjunction */
    const RamCondition& getRHS() const {
        return *rhs;
    }

    void print(std::ostream& os) const override {
        os << "(" << *lhs << " AND " << *rhs << ")";
    }

    std::vector<const RamNode*> getChildNodes() const override {
        return {lhs.get(), rhs.get()};
    }

    RamConjunction* clone() const override {
        return new RamConjunction(
                std::unique_ptr<RamCondition>(lhs->clone()), std::unique_ptr<RamCondition>(rhs->clone()));
    }

    void apply(const RamNodeMapper& map) override {
        lhs = map(std::move(lhs));
        rhs = map(std::move(rhs));
        assert(lhs != nullptr && "left-hand side of conjunction is a nullptr");
        assert(rhs != nullptr && "right-hand side of conjunction is a nullptr");
    }

protected:
    /** Left-hand side of conjunction */
    std::unique_ptr<RamCondition> lhs;

    /** Right-hand side of conjunction */
    std::unique_ptr<RamCondition> rhs;

    bool equal(const RamNode& node) const override {
        assert(nullptr != dynamic_cast<const RamConjunction*>(&node));
        const auto& other = static_cast<const RamConjunction&>(node);
        return getLHS() == other.getLHS() && getRHS() == other.getRHS();
    }
};

/**
 * @class RamNegation
 * @brief Negates a given condition
 *
 * For example:
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * (NOT t0 IN A)
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~
 */
class RamNegation : public RamCondition {
public:
    RamNegation(std::unique_ptr<RamCondition> op) : operand(std::move(op)) {
        assert(nullptr != operand && "operand of negation is a null-pointer");
    }

    /** @brief Get operand of negation */
    const RamCondition& getOperand() const {
        return *operand;
    }

    void print(std::ostream& os) const override {
        os << "(NOT " << *operand << ")";
    }

    std::vector<const RamNode*> getChildNodes() const override {
        return {operand.get()};
    }

    RamNegation* clone() const override {
        return new RamNegation(std::unique_ptr<RamCondition>(operand->clone()));
    }

    void apply(const RamNodeMapper& map) override {
        operand = map(std::move(operand));
        assert(nullptr != operand && "operand of negation is a null-pointer");
    }

protected:
    /** Operand */
    std::unique_ptr<RamCondition> operand;

    bool equal(const RamNode& node) const override {
        assert(nullptr != dynamic_cast<const RamNegation*>(&node));
        const auto& other = static_cast<const RamNegation&>(node);
        return getOperand() == other.getOperand();
    }
};

/**
 * @class RamConstraint
 * @brief Evaluates a binary constraint with respect to two RamExpressions
 *
 * Condition is true if the constraint (a logical operator
 * such as "<") holds between the two operands
 *
 * The following example checks the equality of
 * the two given tuple elements:
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * t0.1 = t1.0
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~
 */
class RamConstraint : public RamCondition {
public:
    RamConstraint(BinaryConstraintOp op, std::unique_ptr<RamExpression> l, std::unique_ptr<RamExpression> r)
            : op(op), lhs(std::move(l)), rhs(std::move(r)) {
        assert(lhs != nullptr && "left-hand side of constraint is a null-pointer");
        assert(rhs != nullptr && "right-hand side of constraint is a null-pointer");
    }

    void print(std::ostream& os) const override {
        os << "(" << *lhs << " ";
        os << toBinaryConstraintSymbol(op);
        os << " " << *rhs << ")";
    }

    /** @brief Get left-hand side */
    const RamExpression& getLHS() const {
        return *lhs;
    }

    /** @brief Get right-hand side */
    const RamExpression& getRHS() const {
        return *rhs;
    }

    /** @brief Get operator symbol */
    BinaryConstraintOp getOperator() const {
        return op;
    }

    std::vector<const RamNode*> getChildNodes() const override {
        return {lhs.get(), rhs.get()};
    }

    RamConstraint* clone() const override {
        return new RamConstraint(op, std::unique_ptr<RamExpression>(lhs->clone()),
                std::unique_ptr<RamExpression>(rhs->clone()));
    }

    void apply(const RamNodeMapper& map) override {
        lhs = map(std::move(lhs));
        rhs = map(std::move(rhs));
        assert(lhs != nullptr && "left-hand side of constraint is a null-pointer");
        assert(rhs != nullptr && "right-hand side of constraint is a null-pointer");
    }

protected:
    /** Operator */
    BinaryConstraintOp op;

    /** Left-hand side of constraint*/
    std::unique_ptr<RamExpression> lhs;

    /** Right-hand side of constraint */
    std::unique_ptr<RamExpression> rhs;

    bool equal(const RamNode& node) const override {
        assert(nullptr != dynamic_cast<const RamConstraint*>(&node));
        const auto& other = static_cast<const RamConstraint&>(node);
        return getOperator() == other.getOperator() && getLHS() == other.getLHS() &&
               getRHS() == other.getRHS();
    }
};

/**
 * @class RamAbstractExistenceCheck
 * @brief Abstract existence check for a tuple in a relation
 */
class RamAbstractExistenceCheck : public RamCondition {
public:
    RamAbstractExistenceCheck(
            std::unique_ptr<RamRelationReference> relRef, std::vector<std::unique_ptr<RamExpression>> vals)
            : relationRef(std::move(relRef)), values(std::move(vals)) {
        assert(relationRef != nullptr && "Relation reference is a nullptr");
    }

    /** @brief Get relation */
    const RamRelation& getRelation() const {
        return *relationRef->get();
    }

    /**
     *  @brief Get arguments of the tuple/pattern
     *  A null pointer element in the vector denotes an unspecified
     *  pattern for a tuple element.
     */
    const std::vector<RamExpression*> getValues() const {
        return toPtrVector(values);
    }

    void print(std::ostream& os) const override {
        os << "("
           << join(values, ",",
                      [](std::ostream& out, const std::unique_ptr<RamExpression>& value) {
                          if (!value) {
                              out << "_";
                          } else {
                              out << *value;
                          }
                      })
           << ") ∈ " << getRelation().getName();
    }

    std::vector<const RamNode*> getChildNodes() const override {
        std::vector<const RamNode*> res = {relationRef.get()};
        for (const auto& cur : values) {
            res.push_back(cur.get());
        }
        return res;
    }

    void apply(const RamNodeMapper& map) override {
        relationRef = map(std::move(relationRef));
        for (auto& val : values) {
            val = map(std::move(val));
        }
        assert(relationRef != nullptr && "Relation reference is a nullptr");
    }

protected:
    /** Relation */
    std::unique_ptr<RamRelationReference> relationRef;

    /** Pattern -- nullptr if undefined */
    std::vector<std::unique_ptr<RamExpression>> values;

    bool equal(const RamNode& node) const override {
        assert(nullptr != dynamic_cast<const RamAbstractExistenceCheck*>(&node));
        const auto& other = static_cast<const RamAbstractExistenceCheck&>(node);
        return getRelation() == other.getRelation() && equal_targets(values, other.values);
    }
};

/**
 * @class RamExistenceCheck
 * @brief Existence check for a tuple(-pattern) in a relation
 *
 * Returns true if the tuple is in the relation
 *
 * The following condition is evaluated to true if the
 * tuple element t0.1 is in the relation A:
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * t0.1 IN A
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~
 */
class RamExistenceCheck : public RamAbstractExistenceCheck {
public:
    RamExistenceCheck(
            std::unique_ptr<RamRelationReference> relRef, std::vector<std::unique_ptr<RamExpression>> vals)
            : RamAbstractExistenceCheck(std::move(relRef), std::move(vals)) {}

    RamExistenceCheck* clone() const override {
        std::vector<std::unique_ptr<RamExpression>> newValues;
        for (auto& cur : values) {
            newValues.emplace_back(cur->clone());
        }
        return new RamExistenceCheck(
                std::unique_ptr<RamRelationReference>(relationRef->clone()), std::move(newValues));
    }
};

/**
 * @class RamProvenanceExistenceCheck
 * @brief Provenance Existence check for a relation
 */
class RamProvenanceExistenceCheck : public RamAbstractExistenceCheck {
public:
    RamProvenanceExistenceCheck(
            std::unique_ptr<RamRelationReference> relRef, std::vector<std::unique_ptr<RamExpression>> vals)
            : RamAbstractExistenceCheck(std::move(relRef), std::move(vals)) {}

    void print(std::ostream& os) const override {
        os << "prov";
        RamAbstractExistenceCheck::print(os);
    }

    RamProvenanceExistenceCheck* clone() const override {
        std::vector<std::unique_ptr<RamExpression>> newValues;
        for (auto& cur : values) {
            newValues.emplace_back(cur->clone());
        }
        return new RamProvenanceExistenceCheck(
                std::unique_ptr<RamRelationReference>(relationRef->clone()), std::move(newValues));
    }
};

/**
 * @class RamEmptinessCheck
 * @brief Emptiness check for a relation
 *
 * Evaluates to true if the given relation is the empty set
 *
 * For example:
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * (B = ∅)
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~
 */
class RamEmptinessCheck : public RamCondition {
public:
    RamEmptinessCheck(std::unique_ptr<RamRelationReference> relRef) : relationRef(std::move(relRef)) {
        assert(relationRef != nullptr && "Relation reference is a nullptr");
    }

    /** @brief Get relation */
    const RamRelation& getRelation() const {
        return *relationRef->get();
    }

    void print(std::ostream& os) const override {
        os << "(" << getRelation().getName() << " = ∅)";
    }

    std::vector<const RamNode*> getChildNodes() const override {
        return {relationRef.get()};
    }

    RamEmptinessCheck* clone() const override {
        return new RamEmptinessCheck(std::unique_ptr<RamRelationReference>(relationRef->clone()));
    }

    void apply(const RamNodeMapper& map) override {
        relationRef = map(std::move(relationRef));
        assert(relationRef != nullptr && "Relation reference is a nullptr");
    }

protected:
    /** Relation */
    std::unique_ptr<RamRelationReference> relationRef;

    bool equal(const RamNode& node) const override {
        assert(nullptr != dynamic_cast<const RamEmptinessCheck*>(&node));
        const auto& other = static_cast<const RamEmptinessCheck&>(node);
        return getRelation() == other.getRelation();
    }
};

/**
 * @brief Convert terms of a conjunction to a list
 * @param conds A RAM condition
 * @return A list of RAM conditions
 *
 * Convert a condition of the format C1 /\ C2 /\ ... /\ Cn
 * to a list {C1, C2, ..., Cn}.
 */
inline std::vector<std::unique_ptr<RamCondition>> toConjunctionList(const RamCondition* condition) {
    std::vector<std::unique_ptr<RamCondition>> conditionList;
    std::queue<const RamCondition*> conditionsToProcess;
    if (condition != nullptr) {
        conditionsToProcess.push(condition);
        while (!conditionsToProcess.empty()) {
            condition = conditionsToProcess.front();
            conditionsToProcess.pop();
            if (const auto* ramConj = dynamic_cast<const RamConjunction*>(condition)) {
                conditionsToProcess.push(&ramConj->getLHS());
                conditionsToProcess.push(&ramConj->getRHS());
            } else {
                conditionList.emplace_back(condition->clone());
            }
        }
    }
    return conditionList;
}

/**
 * @brief Convert list of conditions to a conjunction
 * @param A list of RAM conditions
 * @param A RAM condition
 *
 * Convert a list {C1, C2, ..., Cn} to a condition of
 * the format C1 /\ C2 /\ ... /\ Cn.
 */
inline std::unique_ptr<RamCondition> toCondition(const std::vector<const RamCondition*>& conds) {
    std::unique_ptr<RamCondition> result;
    for (const RamCondition* cur : conds) {
        if (result == nullptr) {
            result = std::unique_ptr<RamCondition>(cur->clone());
        } else {
            result = std::make_unique<RamConjunction>(
                    std::move(result), std::unique_ptr<RamCondition>(cur->clone()));
        }
    }
    return result;
}

/**
 * @brief Convert list of conditions to a conjunction
 * @param conds A list of RAM conditions
 * @return A RAM condition
 *
 * Convert a list {C1, C2, ..., Cn} to a condition of
 * the format C1 /\ C2 /\ ... /\ Cn.
 */
inline std::unique_ptr<RamCondition> toCondition(const std::vector<std::unique_ptr<RamCondition>>& conds) {
    std::vector<const RamCondition*> args;
    for (const auto& cur : conds) {
        args.push_back(cur.get());
    }
    return toCondition(args);
}

}  // end of namespace souffle
