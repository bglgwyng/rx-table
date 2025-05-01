import { SqliteStorage } from "./SqliteStorage.mjs";

// 사용자 타입 정의
type User = {
	id: number;
	name: string;
	email: string;
	age: number;
	created_at: string;
};

// 제약 조건 정의
const userConstraint = {
	primaryKey: ["id"] as const,
};

// 데이터베이스 파일 경로와 테이블 이름으로 SqliteStorage 인스턴스 생성
const userStorage = new SqliteStorage<User, typeof userConstraint>(
	"./database.sqlite",
	"users",
	userConstraint,
);

// 테이블 생성 (없는 경우)
userStorage.createTable(
	{
		id: "INTEGER",
		name: "TEXT NOT NULL",
		email: "TEXT UNIQUE NOT NULL",
		age: "INTEGER",
		created_at: "TEXT DEFAULT CURRENT_TIMESTAMP",
	},
	{
		ifNotExists: true,
	},
);

// 사용자 추가
userStorage.insert({
	id: 1,
	name: "홍길동",
	email: "hong@example.com",
	age: 30,
	created_at: new Date().toISOString(),
});

// 사용자 업데이트 또는 추가
userStorage.upsert({
	id: 2,
	name: "김철수",
	email: "kim@example.com",
	age: 25,
	created_at: new Date().toISOString(),
});

// 사용자 부분 업데이트
async function updateUser() {
	await userStorage.update(["id"], {
		id: 1,
		name: "홍길동 (수정됨)",
		age: 31,
	});
	console.log("사용자 업데이트 완료");
}

// 사용자 삭제
async function deleteUser() {
	await userStorage.delete(["id"]);
	console.log("사용자 삭제 완료");
}

// 비동기 함수 실행
async function main() {
	try {
		await updateUser();
		// await deleteUser(); // 필요시 주석 해제
	} catch (error) {
		console.error("오류 발생:", error);
	} finally {
		// 데이터베이스 연결 종료
		userStorage.close();
	}
}

main();
